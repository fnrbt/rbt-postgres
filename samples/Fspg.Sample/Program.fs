module Fspg.Program

open System
open Fspg

// ---- Pretty-printing a result set ------------------------------------------

let private renderCell (v: string option) =
    match v with
    | None -> "∅" // NULL
    | Some s -> s

let printResult (rs: ResultSet) =
    if rs.Columns.Length = 0 then
        printfn "  (%s)" rs.CommandTag
    else
        let headers = rs.Columns |> Array.map (fun c -> c.Name)
        let body = rs.Rows |> List.map (Array.map renderCell)
        let widths =
            headers
            |> Array.mapi (fun i h ->
                let cellMax =
                    body
                    |> List.map (fun row -> if i < row.Length then row.[i].Length else 0)
                    |> fun xs -> if List.isEmpty xs then 0 else List.max xs
                max h.Length cellMax)
        let pad (s: string) (w: int) = s + String(' ', w - s.Length)
        let line cells =
            cells
            |> Array.mapi (fun i (c: string) -> pad c widths.[i])
            |> String.concat " | "
        printfn "  %s" (line headers)
        printfn "  %s" (widths |> Array.map (fun w -> String('-', w)) |> String.concat "-+-")
        for row in body do
            printfn "  %s" (line row)
        printfn "  (%d row(s); %s)" rs.Rows.Length rs.CommandTag

let private section title = printfn "\n=== %s ===" title

// ---- Configuration ---------------------------------------------------------

let private envOr name fallback =
    match Environment.GetEnvironmentVariable name with
    | null | "" -> fallback
    | v -> v

/// Parse "--key value" / "--key=value" pairs into a map.
let private parseArgs (argv: string[]) =
    let mutable map = Map.empty
    let mutable i = 0
    while i < argv.Length do
        let a = argv.[i]
        if a.StartsWith("--") then
            let key = a.Substring(2)
            match key.IndexOf('=') with
            | -1 ->
                if i + 1 < argv.Length then
                    map <- Map.add (key) argv.[i + 1] map
                    i <- i + 1
                else
                    map <- Map.add key "true" map
            | eq ->
                map <- Map.add (key.Substring(0, eq)) (key.Substring(eq + 1)) map
        i <- i + 1
    map

let private buildConfig (argv: string[]) =
    let args = parseArgs argv
    let get k env fallback =
        match Map.tryFind k args with
        | Some v -> v
        | None -> envOr env fallback
    let user = get "user" "FSPG_USER" "tester"
    let password = get "password" "FSPG_PASSWORD" "secret"
    let database = get "db" "FSPG_DB" "testdb"
    let port = get "port" "FSPG_PORT" "5432" |> int

    // A Unix socket is used when --socket / FSPG_SOCKET is set. The value may be
    // a full socket path or a directory (PostgreSQL convention: the socket file
    // is <dir>/.s.PGSQL.<port>).
    let socketArg =
        match Map.tryFind "socket" args with
        | Some v -> Some v
        | None ->
            match Environment.GetEnvironmentVariable "FSPG_SOCKET" with
            | null | "" -> None
            | v -> Some v

    let endpoint =
        match socketArg with
        | Some s ->
            let path =
                if s.Contains(".s.PGSQL.") then s
                else IO.Path.Combine(s, $".s.PGSQL.{port}")
            Unix path
        | None -> Tcp(get "host" "FSPG_HOST" "127.0.0.1", port)

    { Endpoint = endpoint
      User = user
      Password = password
      Database = database }

// ---- Test scenarios --------------------------------------------------------

let private runScenarios (conn: Connection) =
    section "Server identity"
    for rs in conn.Query("SELECT version()") do
        printResult rs
    printfn "  backend pid: %d" conn.BackendProcessId
    for k in [ "server_version"; "server_encoding"; "client_encoding"; "DateStyle" ] do
        match conn.ServerParameters.TryGetValue k with
        | true, v -> printfn "  param %-16s = %s" k v
        | _ -> ()

    section "Simple query protocol: assorted scalar types"
    for rs in conn.Query(
                  "SELECT 42::int4              AS int_col,
                          'hello world'::text   AS text_col,
                          true                  AS bool_col,
                          3.14159::numeric      AS num_col,
                          NULL::text            AS null_col,
                          now()::date           AS date_col,
                          ARRAY[1,2,3]::int4[]  AS array_col") do
        printResult rs

    section "DDL + DML round-trip"
    conn.Query("DROP TABLE IF EXISTS fspg_demo") |> ignore
    conn.Query(
        "CREATE TABLE fspg_demo (
            id    serial PRIMARY KEY,
            name  text NOT NULL,
            score int,
            tags  text[])") |> ignore
    printfn "  created table fspg_demo"
    for rs in conn.Query(
                  "INSERT INTO fspg_demo (name, score, tags) VALUES
                       ('alice', 10, ARRAY['a','b']),
                       ('bob',   20, ARRAY['c']),
                       ('carol', NULL, NULL)
                   RETURNING id, name, score") do
        printResult rs

    section "Extended query protocol (parameterized, $1/$2)"
    let rs =
        conn.Execute(
            "SELECT id, name, score FROM fspg_demo
             WHERE score >= $1 OR score IS NULL
             ORDER BY id",
            [ Some "15" ])
    printResult rs

    section "Extended query protocol (text + int params, RETURNING)"
    let inserted =
        conn.Execute(
            "INSERT INTO fspg_demo (name, score) VALUES ($1, $2) RETURNING id, name, score",
            [ Some "dave"; Some "99" ])
    printResult inserted

    section "Aggregate"
    for rs in conn.Query(
                  "SELECT count(*) AS n, avg(score)::numeric(10,2) AS avg_score,
                          max(name) AS last_name FROM fspg_demo") do
        printResult rs

    section "Typed decoding (binary results → native .NET types)"
    let typed =
        conn.QueryTyped(
            "SELECT 42::int4 AS i, 3.5::float8 AS f, true AS b,
                    '2026-06-24'::date AS d, gen_random_uuid() AS u")
    let trow = typed.Rows |> List.head
    printfn "  i=%d (fmt %d)  f=%g  b=%b  d=%A  u=%A"
        (trow.GetInt32(trow.GetOrdinal "i"))
        typed.Columns.[0].Format
        (trow.GetDouble(trow.GetOrdinal "f"))
        (trow.GetBool(trow.GetOrdinal "b"))
        (trow.Get<System.DateOnly>(trow.GetOrdinal "d"))
        (trow.GetGuid(trow.GetOrdinal "u"))

    section "Error handling (intentional bad SQL)"
    try
        conn.Query("SELECT * FROM table_that_does_not_exist") |> ignore
        printfn "  ERROR: expected an exception but none was thrown"
    with
    | :? PostgresException as ex ->
        printfn "  caught expected error: %s (constraint=%A)" ex.Message ex.ConstraintName

    section "Recovery after error (connection still usable)"
    for rs in conn.Query("SELECT 'still alive' AS status") do
        printResult rs

    conn.Query("DROP TABLE IF EXISTS fspg_demo") |> ignore

// ---- Entry point -----------------------------------------------------------

[<EntryPoint>]
let main argv =
    let config = buildConfig argv
    let where =
        match config.Endpoint with
        | Tcp(h, p) -> $"tcp {h}:{p}"
        | Unix path -> $"unix {path}"
    printfn "fspg — F# PostgreSQL wire-protocol client"
    printfn "connecting: %s  user=%s db=%s" where config.User config.Database
    try
        use conn = new Connection(config)
        let args = parseArgs argv
        match Map.tryFind "sslmode" args with
        | Some m ->
            conn.SslMode <-
                match m with
                | "disable" -> Disable
                | "allow" -> Allow
                | "require" -> Require
                | "verify-ca" -> VerifyCa
                | "verify-full" -> VerifyFull
                | _ -> Prefer
        | None -> ()
        match Map.tryFind "sslrootcert" args with
        | Some p -> conn.SslRootCert <- Some p
        | None -> ()
        conn.Open()
        printfn "connected and authenticated ✓ (tls=%b mechanism=%s)" conn.IsTlsActive conn.AuthMechanism
        runScenarios conn
        printfn "\nAll scenarios completed ✓"
        0
    with
    | :? PostgresException as ex ->
        eprintfn "\nPostgres error: %s" ex.Message
        1
    | ex ->
        eprintfn "\nFailed: %s" ex.Message
        eprintfn "%s" ex.StackTrace
        2
