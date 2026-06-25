namespace Fspg.Tests

open System
open Fspg

/// Builds the test connection configuration from environment variables, with
/// defaults matching the container provisioned by tests/setup-postgres.sh.
module TestConfig =

    let private envOr name fallback =
        match Environment.GetEnvironmentVariable name with
        | null | "" -> fallback
        | v -> v

    let port () = envOr "FSPG_PORT" "55432" |> int

    let fromEnv () : ConnConfig =
        let p = port ()
        let endpoint =
            match Environment.GetEnvironmentVariable "FSPG_SOCKET" with
            | null | "" -> Tcp(envOr "FSPG_HOST" "127.0.0.1", p)
            | s ->
                let path =
                    if s.Contains(".s.PGSQL.") then s
                    else IO.Path.Combine(s, sprintf ".s.PGSQL.%d" p)
                Unix path
        { Endpoint = endpoint
          User = envOr "FSPG_USER" "tester"
          Password = envOr "FSPG_PASSWORD" "secret"
          Database = envOr "FSPG_DB" "testdb" }

    let describe (c: ConnConfig) =
        match c.Endpoint with
        | Tcp(h, p) -> sprintf "tcp %s:%d" h p
        | Unix path -> sprintf "unix %s" path

    /// Walk up from the test assembly to the repo root (the fspg solution file).
    let repoRoot () =
        let isRoot (d: IO.DirectoryInfo) =
            [ "fspg.slnx"; "fspg.sln" ]
            |> List.exists (fun f -> IO.File.Exists(IO.Path.Combine(d.FullName, f)))
        let mutable dir = IO.DirectoryInfo(AppContext.BaseDirectory)
        while not (isNull dir) && not (isRoot dir) do
            dir <- dir.Parent
        if isNull dir then failwith "repo root (fspg solution) not found" else dir.FullName

    /// Path to the test CA root certificate produced by setup-postgres.sh.
    let rootCert () = IO.Path.Combine(repoRoot (), "tests", "certs", "root.crt")

    /// True when the configured endpoint is TCP (TLS tests require TCP).
    let isTcp (c: ConnConfig) =
        match c.Endpoint with
        | Tcp _ -> true
        | Unix _ -> false
