namespace Fspg

open System
open System.Buffers.Binary
open System.Collections.Generic
open System.Net.Sockets
open System.Security.Cryptography
open System.Text
open System.Threading
open System.Threading.Tasks
open Fspg.Wire
open Fspg.Messages

/// Where to reach the server: a TCP host/port or a Unix-domain socket.
/// The wire protocol is identical over both transports.
type Endpoint =
    | Tcp of host: string * port: int
    | Unix of socketPath: string

/// Connection parameters.
type ConnConfig =
    { Endpoint: Endpoint
      User: string
      Password: string
      Database: string }

/// A fully decoded result set from one statement (text format).
type ResultSet =
    { Columns: FieldDescription[]
      Rows: string option [] list
      CommandTag: string }

/// An async LISTEN/NOTIFY notification.
type Notification =
    { Pid: int
      Channel: string
      Payload: string }

module private Md5 =
    let private toHex (b: byte[]) =
        let sb = StringBuilder(b.Length * 2)
        for x in b do
            sb.Append(x.ToString("x2")) |> ignore
        sb.ToString()

    let private md5Hex (data: byte[]) =
        use md5 = MD5.Create()
        toHex (md5.ComputeHash data)

    /// PostgreSQL MD5: "md5" + md5( md5(password + user) + salt ).
    let response (user: string) (password: string) (salt: byte[]) =
        let inner = md5Hex (utf8.GetBytes(password + user))
        let outer = md5Hex (Array.append (utf8.GetBytes inner) salt)
        "md5" + outer

module private ConnUtil =
    /// Build a CTS linked to the caller's token that also fires after `timeout`
    /// (a non-positive timeout means "no timeout").
    let linked (ct: CancellationToken) (timeout: TimeSpan) =
        let cts = CancellationTokenSource.CreateLinkedTokenSource(ct)
        if timeout > TimeSpan.Zero then cts.CancelAfter(timeout)
        cts

/// A server-side prepared statement, with the metadata learned from Describe.
type PreparedStatement =
    { Name: string
      Sql: string
      ParameterOids: int[]
      Columns: FieldDescription[] }

/// Pull-based async cursor over a portal, fetched in bounded batches via the
/// extended protocol's row-limited Execute (so large results never fully
/// materialize). Implements IAsyncEnumerable<PgRow>.
type PgRowStream
    (transport: Transport,
     sql: string,
     parameters: obj option list,
     batchSize: int,
     encoding: System.Text.Encoding,
     ct: CancellationToken) =

    let decodeRow (pgcols: PgColumn[]) (m: IncomingMessage) =
        let raw = parseDataRow m
        let vals =
            raw
            |> Array.mapi (fun i cell ->
                cell |> Option.map (fun bytes -> Codecs.decode encoding pgcols.[i].TypeOid pgcols.[i].Format bytes))
        PgRow(pgcols, vals)

    member private _.DrainToReady() : Task =
        task {
            let mutable z = false
            while not z do
                let! m = transport.ReadMessageAsync(ct)
                if m.Tag = 'Z' then z <- true
        }

    member this.GetEnumerator() : IAsyncEnumerator<PgRow> =
        let buffer = Queue<PgRow>()
        let mutable pgcols : PgColumn[] = [||]
        let mutable initialized = false
        let mutable portalDone = false
        let mutable current = Unchecked.defaultof<PgRow>

        let raiseError (e: PostgresErrorFields) : Task =
            task {
                do! transport.SendAsync(sync (), ct)
                do! this.DrainToReady()
                raise (PostgresException e)
            }

        // Parse + Describe + Bind, leaving the portal ready for row-limited Execute.
        let initAsync () : Task =
            task {
                transport.Enqueue(parse encoding "" sql [])
                transport.Enqueue(describeStatement "")
                transport.Enqueue(flush ())
                do! transport.FlushAsync(ct)
                let mutable columns : FieldDescription[] = [||]
                let mutable paramOids : int[] = [||]
                let mutable describing = true
                while describing do
                    let! m = transport.ReadMessageAsync(ct)
                    match m.Tag with
                    | '1' -> ()
                    | 't' -> paramOids <- parseParameterDescription m
                    | 'T' -> columns <- parseRowDescription m; describing <- false
                    | 'n' -> columns <- [||]; describing <- false
                    | 'E' -> do! raiseError (ErrorFields.parse m)
                    | other -> failwithf "Unexpected '%c' during stream Describe." other
                let formats = columns |> Array.map (fun c -> if Codecs.hasBinary c.TypeOid then 1 else 0)
                pgcols <- columns |> Array.mapi (fun i c -> { Name = c.Name; TypeOid = c.TypeOid; Format = formats.[i] })
                let oidOf i = if i < paramOids.Length then paramOids.[i] else 0
                let paramData =
                    parameters |> List.mapi (fun i p -> p |> Option.map (fun v -> Codecs.encodeParam encoding (oidOf i) v))
                transport.Enqueue(bindParams "" "" paramData formats)
                transport.Enqueue(flush ())
                do! transport.FlushAsync(ct)
                let! bc = transport.ReadMessageAsync(ct)
                match bc.Tag with
                | '2' -> ()
                | 'E' -> do! raiseError (ErrorFields.parse bc)
                | other -> failwithf "Unexpected '%c' after stream Bind." other
            }

        // Fetch the next batch; on CommandComplete close out with Sync.
        let pullBatchAsync () : Task =
            task {
                transport.Enqueue(execute "" batchSize)
                transport.Enqueue(flush ())
                do! transport.FlushAsync(ct)
                let mutable batching = true
                while batching do
                    let! m = transport.ReadMessageAsync(ct)
                    match m.Tag with
                    | 'D' -> buffer.Enqueue(decodeRow pgcols m)
                    | 's' -> batching <- false // PortalSuspended: more rows remain
                    | 'C' -> // CommandComplete: portal exhausted
                        m.CString() |> ignore
                        portalDone <- true
                        batching <- false
                        do! transport.SendAsync(sync (), ct)
                        do! this.DrainToReady()
                    | 'N' -> eprintfn "NOTICE: %s" (ErrorFields.format (ErrorFields.parse m))
                    | 'E' -> batching <- false; do! raiseError (ErrorFields.parse m)
                    | other -> failwithf "Unexpected '%c' while streaming rows." other
            }

        { new IAsyncEnumerator<PgRow> with
            member _.Current = current

            member _.MoveNextAsync() : ValueTask<bool> =
                ValueTask<bool>(
                    task {
                        if not initialized then
                            do! initAsync ()
                            initialized <- true
                        if buffer.Count = 0 && not portalDone then
                            do! pullBatchAsync ()
                        if buffer.Count > 0 then
                            current <- buffer.Dequeue()
                            return true
                        else
                            return false
                    })

            member _.DisposeAsync() : ValueTask =
                ValueTask(
                    task {
                        // If the consumer abandoned the stream early, close the
                        // portal and return the connection to a clean state.
                        if initialized && not portalDone then
                            try
                                do! transport.SendAsync(sync (), ct)
                                do! this.DrainToReady()
                            with _ -> ()
                    }) }

    interface IAsyncEnumerable<PgRow> with
        member this.GetAsyncEnumerator(_: CancellationToken) = this.GetEnumerator()

/// A live connection to a PostgreSQL server.
type Connection(config: ConnConfig) =
    let mutable socket : Socket = null
    let mutable transport : Transport = Unchecked.defaultof<Transport>
    let parameters = Dictionary<string, string>()
    let mutable backendPid = 0
    let mutable backendKey = 0
    let mutable tlsActive = false
    let mutable tlsServerCert : System.Security.Cryptography.X509Certificates.X509Certificate2 option = None
    let mutable stmtCounter = 0
    let stmtCache = Dictionary<string, string>() // sql -> prepared statement name
    let notifications = Queue<Notification>()
    let notifyEvent = Event<Notification>()
    let mutable dataEncoding : System.Text.Encoding = utf8 // resolved from client_encoding

    let applyParameterStatus (k: string) (v: string) =
        parameters.[k] <- v
        if k = "client_encoding" then dataEncoding <- PgEncoding.resolve v

    let recordNotification (m: IncomingMessage) =
        let pid, ch, payload = parseNotification m
        let n = { Pid = pid; Channel = ch; Payload = payload }
        notifications.Enqueue n
        notifyEvent.Trigger n

    member _.Config = config
    member _.ServerParameters = parameters
    /// The .NET encoding resolved from the server's client_encoding.
    member _.DataEncoding = dataEncoding
    member _.BackendProcessId = backendPid
    member _.BackendSecretKey = backendKey
    member _.IsTlsActive = tlsActive

    /// Cheap liveness check (no round-trip) for an *idle* connection: a healthy
    /// idle connection (drained to ReadyForQuery, not LISTENing) has nothing
    /// pending to read, so Poll(SelectRead) is false. If it reports readable, the
    /// peer has closed or sent an unexpected message (e.g. a terminate FATAL) —
    /// treat it as dead. Used by the pool to avoid handing out dead connections.
    member _.IsHealthy =
        try
            not (isNull socket)
            && socket.Connected
            && not (socket.Poll(0, SelectMode.SelectRead))
        with _ ->
            false
    /// The negotiated server certificate (for SCRAM channel binding); internal use.
    member _.TlsServerCertificate = tlsServerCert
    /// The SASL mechanism actually used ("SCRAM-SHA-256[-PLUS]"), set during auth.
    member val AuthMechanism = "" with get, set
    /// Test hook: corrupt the channel-binding hash to prove the server enforces it.
    member val TamperChannelBinding = false with get, set
    member val ConnectTimeout = TimeSpan.FromSeconds 15.0 with get, set
    member val CommandTimeout = TimeSpan.FromSeconds 30.0 with get, set
    member val SslMode = Prefer with get, set
    member val SslRootCert : string option = None with get, set
    member val SslClientCertificate :
        System.Security.Cryptography.X509Certificates.X509Certificate2 option = None with get, set
    /// The client_encoding requested at startup (default UTF8). Note: value
    /// decoding currently assumes UTF-8 regardless of this setting.
    member val ClientEncoding = "UTF8" with get, set
    /// Set to Some "database" (logical) or Some "true" (physical) to open a
    /// replication connection.
    member val ReplicationMode : string option = None with get, set

    // ---- Authentication ------------------------------------------------------

    member private this.AuthenticateAsync(ct: CancellationToken) : Task =
        task {
            let mutable authed = false
            // Persisted across GSS continuation messages.
            let mutable gss : System.Net.Security.NegotiateAuthentication option = None
            while not authed do
                let! msg = transport.ReadMessageAsync(ct)
                match msg.Tag with
                | 'R' ->
                    match parseAuth msg with
                    | AuthOk -> authed <- true
                    | AuthCleartextPassword ->
                        do! transport.SendAsync(passwordMessage config.Password, ct)
                    | AuthMD5Password salt ->
                        do! transport.SendAsync(passwordMessage (Md5.response config.User config.Password salt), ct)
                    | AuthGSS
                    | AuthSSPI ->
                        // Best-effort GSSAPI/SSPI (needs a Kerberos KDC; the
                        // message flow is here, but is untested in this env).
                        let host =
                            match config.Endpoint with
                            | Tcp(h, _) -> h
                            | Unix _ -> "localhost"
                        let opts =
                            System.Net.Security.NegotiateAuthenticationClientOptions(
                                TargetName = sprintf "postgres/%s" host)
                        let n = new System.Net.Security.NegotiateAuthentication(opts)
                        gss <- Some n
                        let mutable sc = System.Net.Security.NegotiateAuthenticationStatusCode.ContinueNeeded
                        let blob = n.GetOutgoingBlob(ReadOnlySpan<byte>.Empty, &sc)
                        this.AuthMechanism <- "GSS"
                        do! transport.SendAsync(gssResponse (if isNull blob then [||] else blob), ct)
                    | AuthGSSContinue token ->
                        match gss with
                        | Some n ->
                            let mutable sc = System.Net.Security.NegotiateAuthenticationStatusCode.ContinueNeeded
                            let blob = n.GetOutgoingBlob(ReadOnlySpan<byte>(token), &sc)
                            if not (isNull blob) && blob.Length > 0 then
                                do! transport.SendAsync(gssResponse blob, ct)
                        | None -> failwith "AuthenticationGSSContinue without a GSS handshake in progress."
                    | AuthSASL mechanisms ->
                        // Prefer channel binding (-PLUS) over TLS; protect against
                        // downgrade with the "y,," flag when TLS is up but the
                        // server did not offer -PLUS.
                        let binding, mech =
                            if tlsActive
                               && List.contains "SCRAM-SHA-256-PLUS" mechanisms
                               && tlsServerCert.IsSome then
                                let h = Tls.channelBindingHash tlsServerCert.Value
                                let h = if this.TamperChannelBinding then Array.map (fun b -> b ^^^ 0xFFuy) h else h
                                Scram.CbTlsServerEndPoint h, "SCRAM-SHA-256-PLUS"
                            elif tlsActive && List.contains "SCRAM-SHA-256" mechanisms then
                                Scram.CbNotUsed, "SCRAM-SHA-256"
                            elif List.contains "SCRAM-SHA-256" mechanisms then
                                Scram.CbNone, "SCRAM-SHA-256"
                            else
                                failwith $"Server offered unsupported SASL mechanisms: {String.Join(',', mechanisms)}"
                        this.AuthMechanism <- mech
                        let state, clientFirst = Scram.start config.Password binding
                        do! transport.SendAsync(saslInitialResponse mech clientFirst, ct)
                        let! cont = transport.ReadMessageAsync(ct)
                        if cont.Tag = 'E' then raise (PostgresException(ErrorFields.parse cont))
                        let serverFirst =
                            match parseAuth cont with
                            | AuthSASLContinue d -> d
                            | other -> failwith $"Expected SASLContinue, got {other}"
                        let clientFinal, expectedServerSig = Scram.finish state serverFirst
                        do! transport.SendAsync(saslResponse clientFinal, ct)
                        let! fin = transport.ReadMessageAsync(ct)
                        if fin.Tag = 'E' then raise (PostgresException(ErrorFields.parse fin))
                        let saslFinal =
                            match parseAuth fin with
                            | AuthSASLFinal d -> d
                            | other -> failwith $"Expected SASLFinal, got {other}"
                        Scram.verifyServer expectedServerSig saslFinal
                    | AuthOther n -> failwith $"Unsupported authentication request code {n}."
                    | AuthSASLContinue _
                    | AuthSASLFinal _ -> failwith "Unexpected SASL continuation at top level."
                | 'E' -> raise (PostgresException(ErrorFields.parse msg))
                | other -> failwith $"Unexpected message '{other}' during authentication."
        }

    /// Read the post-auth burst up to and including the first ReadyForQuery.
    member private _.ReadStartupTailAsync(ct: CancellationToken) : Task =
        task {
            let mutable ready = false
            while not ready do
                let! msg = transport.ReadMessageAsync(ct)
                match msg.Tag with
                | 'S' -> // ParameterStatus
                    let k = msg.CString()
                    let v = msg.CString()
                    applyParameterStatus k v
                | 'K' -> // BackendKeyData
                    backendPid <- msg.Int32()
                    backendKey <- msg.Int32()
                | 'N' -> eprintfn "NOTICE: %s" (ErrorFields.format (ErrorFields.parse msg))
                | 'v' -> () // NegotiateProtocolVersion: server downgraded / unknown opts — proceed
                | 'Z' -> ready <- true // ReadyForQuery
                | 'E' -> raise (PostgresException(ErrorFields.parse msg))
                | other -> failwithf "Unexpected message '%c' after authentication." other
        }

    // ---- Connect -------------------------------------------------------------

    member this.OpenAsync(?cancellationToken: CancellationToken) : Task =
        task {
            let ct = defaultArg cancellationToken CancellationToken.None
            use cts = ConnUtil.linked ct this.ConnectTimeout
            let tok = cts.Token
            match config.Endpoint with
            | Tcp(host, port) ->
                socket <- new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
                socket.NoDelay <- true
                do! socket.ConnectAsync(host, port, tok)
            | Unix path ->
                socket <- new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified)
                do! socket.ConnectAsync(UnixDomainSocketEndPoint(path), tok)

            let netStream = new NetworkStream(socket, ownsSocket = true)

            // TLS negotiation (SSLRequest) happens on the raw stream before the
            // Transport buffers anything; the protocol then runs over the result.
            let host =
                match config.Endpoint with
                | Tcp(h, _) -> h
                | Unix _ -> "localhost"
            let sslCfg =
                { Mode = this.SslMode
                  RootCertPath = this.SslRootCert
                  ClientCertificate = this.SslClientCertificate
                  TargetHost = host }
            let! tls = Tls.negotiateAsync (netStream :> System.IO.Stream) sslCfg tok
            tlsActive <- tls.Encrypted
            tlsServerCert <- tls.ServerCertificate
            transport <- new Transport(tls.Stream)

            let startupParams =
                let baseParams =
                    [ "user", config.User
                      "database", config.Database
                      "application_name", "fspg"
                      "client_encoding", this.ClientEncoding ]
                match this.ReplicationMode with
                | Some v -> baseParams @ [ "replication", v ]
                | None -> baseParams
            do! transport.SendAsync(startupMessage startupParams, tok)
            do! this.AuthenticateAsync(tok)
            do! this.ReadStartupTailAsync(tok)
        }

    member this.Open() = this.OpenAsync().GetAwaiter().GetResult()

    // ---- Result collection ---------------------------------------------------

    member private _.CollectUntilReadyAsync(ct: CancellationToken) : Task<ResultSet list> =
        task {
            let results = List<ResultSet>()
            let mutable columns : FieldDescription[] = [||]
            let mutable rows = List<string option []>()
            let mutable pendingError : PostgresErrorFields option = None
            let mutable ready = false

            let decodeRow (raw: byte[] option []) = raw |> Array.map (Option.map dataEncoding.GetString)

            while not ready do
                let! msg = transport.ReadMessageAsync(ct)
                match msg.Tag with
                | 'T' -> // RowDescription
                    columns <- parseRowDescription msg
                    rows <- List<string option []>()
                | 'D' -> rows.Add(decodeRow (parseDataRow msg)) // DataRow
                | 'C' -> // CommandComplete
                    let tag = msg.CString()
                    results.Add({ ResultSet.Columns = columns; Rows = List.ofSeq rows; CommandTag = tag })
                    columns <- [||]
                    rows <- List<string option []>()
                | 'I' -> results.Add({ ResultSet.Columns = [||]; Rows = []; CommandTag = "EMPTY" }) // EmptyQueryResponse
                | 'n' -> () // NoData
                | '1' | '2' | '3' -> () // ParseComplete / BindComplete / CloseComplete
                | 't' -> () // ParameterDescription
                | 's' -> () // PortalSuspended
                | 'S' -> // ParameterStatus (e.g. after SET client_encoding)
                    let k = msg.CString()
                    let v = msg.CString()
                    applyParameterStatus k v
                | 'N' -> eprintfn "NOTICE: %s" (ErrorFields.format (ErrorFields.parse msg))
                | 'A' -> recordNotification msg // NotificationResponse (LISTEN/NOTIFY)
                | 'E' -> pendingError <- Some(ErrorFields.parse msg)
                | 'Z' -> ready <- true
                | other -> failwithf "Unexpected message '%c' while reading results." other

            match pendingError with
            | Some err -> return raise (PostgresException err)
            | None -> return List.ofSeq results
        }

    /// Run a query operation with graceful cancellation: on a fired token (or
    /// command timeout) a server CancelRequest is sent and the query is read to
    /// a clean finish, so the connection stays usable. Surfaces
    /// OperationCanceledException (token) or TimeoutException (timeout).
    member private this.RunCancellableAsync
        (op: CancellationToken -> Task<'T>, userCt: CancellationToken, timeout: TimeSpan)
        : Task<'T> =
        task {
            use cts = ConnUtil.linked userCt timeout
            let mutable cancelSent = 0
            use _reg =
                cts.Token.Register(fun () ->
                    if Interlocked.Exchange(&cancelSent, 1) = 0 then
                        try this.CancelAsync() |> ignore with _ -> ())
            try
                // Reads use None so they finish on the server's clean 57014, not
                // an aborted socket read.
                let! result = op CancellationToken.None
                if userCt.IsCancellationRequested then return raise (OperationCanceledException userCt)
                elif cts.IsCancellationRequested then return raise (TimeoutException "Command timed out.")
                else return result
            with :? PostgresException as ex when ex.SqlState = "57014" && cts.IsCancellationRequested ->
                if userCt.IsCancellationRequested then return raise (OperationCanceledException userCt)
                else return raise (TimeoutException "Command timed out.")
        }

    // ---- Simple query protocol ----------------------------------------------

    member this.QueryAsync(sql: string, ?cancellationToken: CancellationToken) : Task<ResultSet list> =
        let userCt = defaultArg cancellationToken CancellationToken.None
        this.RunCancellableAsync(
            (fun tok ->
                task {
                    do! transport.SendAsync(query dataEncoding sql, tok)
                    return! this.CollectUntilReadyAsync(tok)
                }),
            userCt,
            this.CommandTimeout)

    member this.Query(sql: string) = this.QueryAsync(sql).GetAwaiter().GetResult()

    // ---- Extended query protocol (unnamed, text params) ---------------------

    member this.ExecuteAsync
        (sql: string, parameters: string option list, ?cancellationToken: CancellationToken)
        : Task<ResultSet> =
        let userCt = defaultArg cancellationToken CancellationToken.None
        this.RunCancellableAsync(
            (fun tok ->
                task {
                    transport.Enqueue(parse dataEncoding "" sql [])
                    transport.Enqueue(bind "" "" parameters)
                    transport.Enqueue(describePortal "")
                    transport.Enqueue(execute "" 0)
                    transport.Enqueue(sync ())
                    do! transport.FlushAsync(tok)
                    let! results = this.CollectUntilReadyAsync(tok)
                    return
                        match results with
                        | [ single ] -> single
                        | [] -> { ResultSet.Columns = [||]; Rows = []; CommandTag = "" }
                        | many -> List.last many
                }),
            userCt,
            this.CommandTimeout)

    member this.Execute(sql: string, parameters: string option list) =
        this.ExecuteAsync(sql, parameters).GetAwaiter().GetResult()

    // ---- Typed extended query (binary results where supported) --------------

    /// Run a (optionally parameterized) statement and decode each column to its
    /// natural .NET type. Columns whose type has a binary codec are requested in
    /// binary; the rest in text. Parameters are sent in text format.
    member this.QueryTypedAsync
        (sql: string, ?parameters: obj option list, ?cancellationToken: CancellationToken)
        : Task<TypedResult> =
        let ps = defaultArg parameters []
        let userCt = defaultArg cancellationToken CancellationToken.None
        this.RunCancellableAsync(
          (fun tok ->
            task {

            // 1) Parse + Describe(statement) + Flush to learn the column OIDs.
            transport.Enqueue(parse dataEncoding "" sql [])
            transport.Enqueue(describeStatement "")
            transport.Enqueue(flush ())
            do! transport.FlushAsync(tok)

            let mutable columns : FieldDescription[] = [||]
            let mutable paramOids : int[] = [||]
            let mutable describeErr : PostgresErrorFields option = None
            let mutable describing = true
            while describing do
                let! m = transport.ReadMessageAsync(tok)
                match m.Tag with
                | '1' -> () // ParseComplete
                | 't' -> paramOids <- parseParameterDescription m // param type OIDs
                | 'T' ->
                    columns <- parseRowDescription m
                    describing <- false
                | 'n' ->
                    columns <- [||]
                    describing <- false
                | 'E' ->
                    describeErr <- Some(ErrorFields.parse m)
                    describing <- false
                | other -> failwithf "Unexpected message '%c' during Describe." other

            match describeErr with
            | Some e ->
                // Resynchronize after the parse/describe error, then raise.
                do! transport.SendAsync(sync (), tok)
                let mutable z = false
                while not z do
                    let! m = transport.ReadMessageAsync(tok)
                    if m.Tag = 'Z' then z <- true
                raise (PostgresException e)
            | None -> ()

            // 2) Choose per-column result formats; binary-encode params by OID.
            let formats = columns |> Array.map (fun c -> if Codecs.hasBinary c.TypeOid then 1 else 0)
            let pgcols =
                columns
                |> Array.mapi (fun i c -> { Name = c.Name; TypeOid = c.TypeOid; Format = formats.[i] })
            let oidOf i = if i < paramOids.Length then paramOids.[i] else 0
            let paramData =
                ps |> List.mapi (fun i p -> p |> Option.map (fun v -> Codecs.encodeParam dataEncoding (oidOf i) v))

            // 3) Bind + Execute + Sync.
            transport.Enqueue(bindParams "" "" paramData formats)
            transport.Enqueue(execute "" 0)
            transport.Enqueue(sync ())
            do! transport.FlushAsync(tok)

            let rows = List<PgRow>()
            let mutable tag = ""
            let mutable err : PostgresErrorFields option = None
            let mutable ready = false
            while not ready do
                let! m = transport.ReadMessageAsync(tok)
                match m.Tag with
                | '2' -> () // BindComplete
                | 'D' ->
                    let raw = parseDataRow m
                    let vals =
                        raw
                        |> Array.mapi (fun i cell ->
                            cell |> Option.map (fun bytes -> Codecs.decode dataEncoding pgcols.[i].TypeOid pgcols.[i].Format bytes))
                    rows.Add(PgRow(pgcols, vals))
                | 'C' -> tag <- m.CString()
                | 's' -> () // PortalSuspended (not used here)
                | 'N' -> eprintfn "NOTICE: %s" (ErrorFields.format (ErrorFields.parse m))
                | 'E' -> err <- Some(ErrorFields.parse m)
                | 'Z' -> ready <- true
                | other -> failwithf "Unexpected message '%c' while reading typed rows." other

            match err with
            | Some e -> return raise (PostgresException e)
            | None -> return { TypedResult.Columns = pgcols; Rows = List.ofSeq rows; CommandTag = tag }
          }),
          userCt,
          this.CommandTimeout)

    member this.QueryTyped(sql: string) =
        this.QueryTypedAsync(sql).GetAwaiter().GetResult()

    // ---- Named prepared statements ------------------------------------------

    member private _.ReadExecRowsAsync(pgcols: PgColumn[], tok: CancellationToken) : Task<PgRow list * string> =
        task {
            let rows = List<PgRow>()
            let mutable tag = ""
            let mutable err : PostgresErrorFields option = None
            let mutable ready = false
            while not ready do
                let! m = transport.ReadMessageAsync(tok)
                match m.Tag with
                | '2' -> ()
                | 'D' ->
                    let raw = parseDataRow m
                    let vals =
                        raw
                        |> Array.mapi (fun i cell ->
                            cell |> Option.map (fun bytes -> Codecs.decode dataEncoding pgcols.[i].TypeOid pgcols.[i].Format bytes))
                    rows.Add(PgRow(pgcols, vals))
                | 'C' -> tag <- m.CString()
                | 's' -> ()
                | 'A' -> recordNotification m
                | 'N' -> eprintfn "NOTICE: %s" (ErrorFields.format (ErrorFields.parse m))
                | 'E' -> err <- Some(ErrorFields.parse m)
                | 'Z' -> ready <- true
                | other -> failwithf "Unexpected message '%c' reading rows." other
            match err with
            | Some e -> return raise (PostgresException e)
            | None -> return (List.ofSeq rows, tag)
        }

    /// Parse and Describe a named statement on the server, returning its
    /// parameter OIDs and result column metadata. The name is reusable.
    member this.PrepareAsync(sql: string, ?name: string, ?cancellationToken: CancellationToken) : Task<PreparedStatement> =
        task {
            let ct = defaultArg cancellationToken CancellationToken.None
            use cts = ConnUtil.linked ct this.CommandTimeout
            let tok = cts.Token
            let stmtName =
                match name with
                | Some n -> n
                | None ->
                    stmtCounter <- stmtCounter + 1
                    sprintf "fspg_s%d" stmtCounter
            transport.Enqueue(parse dataEncoding stmtName sql [])
            transport.Enqueue(describeStatement stmtName)
            transport.Enqueue(sync ())
            do! transport.FlushAsync(tok)
            let mutable paramOids : int[] = [||]
            let mutable columns : FieldDescription[] = [||]
            let mutable err : PostgresErrorFields option = None
            let mutable ready = false
            while not ready do
                let! m = transport.ReadMessageAsync(tok)
                match m.Tag with
                | '1' -> ()
                | 't' -> paramOids <- parseParameterDescription m
                | 'T' -> columns <- parseRowDescription m
                | 'n' -> columns <- [||]
                | 'E' -> err <- Some(ErrorFields.parse m)
                | 'Z' -> ready <- true
                | other -> failwithf "Unexpected message '%c' during Prepare." other
            match err with
            | Some e -> return raise (PostgresException e)
            | None ->
                stmtCache.[sql] <- stmtName
                return { Name = stmtName; Sql = sql; ParameterOids = paramOids; Columns = columns }
        }

    member this.Prepare(sql: string) = this.PrepareAsync(sql).GetAwaiter().GetResult()

    /// Bind + Execute a previously prepared statement (no re-Parse).
    member this.ExecutePreparedAsync
        (stmt: PreparedStatement, parameters: obj option list, ?cancellationToken: CancellationToken)
        : Task<TypedResult> =
        let userCt = defaultArg cancellationToken CancellationToken.None
        this.RunCancellableAsync(
          (fun tok ->
            task {
                let formats = stmt.Columns |> Array.map (fun c -> if Codecs.hasBinary c.TypeOid then 1 else 0)
                let pgcols =
                    stmt.Columns
                    |> Array.mapi (fun i c -> { Name = c.Name; TypeOid = c.TypeOid; Format = formats.[i] })
                let oidOf i = if i < stmt.ParameterOids.Length then stmt.ParameterOids.[i] else 0
                let paramData =
                    parameters |> List.mapi (fun i p -> p |> Option.map (fun v -> Codecs.encodeParam dataEncoding (oidOf i) v))
                transport.Enqueue(bindParams "" stmt.Name paramData formats)
                transport.Enqueue(execute "" 0)
                transport.Enqueue(sync ())
                do! transport.FlushAsync(tok)
                let! (rows, tag) = this.ReadExecRowsAsync(pgcols, tok)
                return { TypedResult.Columns = pgcols; Rows = rows; CommandTag = tag }
            }),
          userCt,
          this.CommandTimeout)

    member this.ExecutePrepared(stmt: PreparedStatement, parameters: obj option list) =
        this.ExecutePreparedAsync(stmt, parameters).GetAwaiter().GetResult()

    /// Close a named prepared statement on the server.
    member this.CloseStatementAsync(name: string, ?cancellationToken: CancellationToken) : Task =
        task {
            let ct = defaultArg cancellationToken CancellationToken.None
            use cts = ConnUtil.linked ct this.CommandTimeout
            let tok = cts.Token
            transport.Enqueue(closeStatement name)
            transport.Enqueue(sync ())
            do! transport.FlushAsync(tok)
            let mutable ready = false
            while not ready do
                let! m = transport.ReadMessageAsync(tok)
                match m.Tag with
                | '3' -> () // CloseComplete
                | 'Z' -> ready <- true
                | 'E' -> raise (PostgresException(ErrorFields.parse m))
                | _ -> ()
            stmtCache
            |> Seq.filter (fun kv -> kv.Value = name)
            |> Seq.map (fun kv -> kv.Key)
            |> Seq.toList
            |> List.iter (fun k -> stmtCache.Remove k |> ignore)
        }

    member this.CloseStatement(name: string) =
        this.CloseStatementAsync(name).GetAwaiter().GetResult()

    // ---- Streaming (row-limited, bounded memory) ----------------------------

    /// Stream the rows of a query in bounded batches without materializing the
    /// whole result. The connection must not be used for anything else until the
    /// returned enumerable is fully consumed or disposed.
    member _.Stream
        (sql: string, ?parameters: obj option list, ?batchSize: int, ?cancellationToken: CancellationToken)
        : IAsyncEnumerable<PgRow> =
        let ps = defaultArg parameters []
        let bs = defaultArg batchSize 100
        let ct = defaultArg cancellationToken CancellationToken.None
        PgRowStream(transport, sql, ps, bs, dataEncoding, ct) :> IAsyncEnumerable<PgRow>

    // ---- COPY protocol -------------------------------------------------------

    member private _.DrainToReadyAsync(tok: CancellationToken) : Task =
        task {
            let mutable z = false
            while not z do
                let! m = transport.ReadMessageAsync(tok)
                if m.Tag = 'Z' then z <- true
        }

    /// COPY ... FROM STDIN: stream the given CopyData chunks to the server.
    /// `copySql` is e.g. "COPY t (a,b) FROM STDIN" (text) or "... WITH (FORMAT binary)".
    /// Returns the number of rows copied.
    member this.CopyInRawAsync(copySql: string, chunks: seq<byte[]>, ?cancellationToken: CancellationToken) : Task<int> =
        task {
            let ct = defaultArg cancellationToken CancellationToken.None
            use cts = ConnUtil.linked ct this.CommandTimeout
            let tok = cts.Token
            do! transport.SendAsync(query dataEncoding copySql, tok)

            // Await CopyInResponse 'G' (or an immediate error).
            let mutable starting = true
            let mutable startErr : PostgresErrorFields option = None
            while starting do
                let! m = transport.ReadMessageAsync(tok)
                match m.Tag with
                | 'G' -> starting <- false
                | 'N' -> ()
                | 'E' ->
                    startErr <- Some(ErrorFields.parse m)
                    do! this.DrainToReadyAsync(tok)
                    starting <- false
                | other -> failwithf "Expected CopyInResponse, got '%c'." other

            match startErr with
            | Some e -> return raise (PostgresException e)
            | None ->
                for chunk in chunks do
                    transport.Enqueue(copyData chunk)
                transport.Enqueue(copyDone ())
                do! transport.FlushAsync(tok)

                let mutable tag = ""
                let mutable err : PostgresErrorFields option = None
                let mutable ready = false
                while not ready do
                    let! m = transport.ReadMessageAsync(tok)
                    match m.Tag with
                    | 'C' -> tag <- m.CString()
                    | 'N' -> ()
                    | 'E' -> err <- Some(ErrorFields.parse m)
                    | 'Z' -> ready <- true
                    | _ -> ()
                match err with
                | Some e -> return raise (PostgresException e)
                | None -> return copyCount tag
        }

    /// COPY ... FROM STDIN with one text row per line.
    member this.CopyInTextAsync(copySql: string, rows: seq<string>, ?cancellationToken: CancellationToken) : Task<int> =
        let chunks = rows |> Seq.map (fun r -> utf8.GetBytes(r + "\n"))
        this.CopyInRawAsync(copySql, chunks, ?cancellationToken = cancellationToken)

    member this.CopyInText(copySql: string, rows: seq<string>) =
        this.CopyInTextAsync(copySql, rows).GetAwaiter().GetResult()

    /// COPY ... TO STDOUT: collect the raw CopyData chunks the server sends.
    member this.CopyOutRawAsync(copySql: string, ?cancellationToken: CancellationToken) : Task<byte[] list> =
        task {
            let ct = defaultArg cancellationToken CancellationToken.None
            use cts = ConnUtil.linked ct this.CommandTimeout
            let tok = cts.Token
            do! transport.SendAsync(query dataEncoding copySql, tok)
            let chunks = List<byte[]>()
            let mutable err : PostgresErrorFields option = None
            let mutable ready = false
            while not ready do
                let! m = transport.ReadMessageAsync(tok)
                match m.Tag with
                | 'H' -> () // CopyOutResponse
                | 'd' -> chunks.Add(m.Body) // CopyData
                | 'c' -> () // CopyDone
                | 'C' -> m.CString() |> ignore // CommandComplete
                | 'N' -> ()
                | 'E' -> err <- Some(ErrorFields.parse m)
                | 'Z' -> ready <- true
                | _ -> ()
            match err with
            | Some e -> return raise (PostgresException e)
            | None -> return List.ofSeq chunks
        }

    /// COPY ... TO STDOUT decoded as text lines.
    member this.CopyOutTextAsync(copySql: string, ?cancellationToken: CancellationToken) : Task<string list> =
        task {
            let! chunks = this.CopyOutRawAsync(copySql, ?cancellationToken = cancellationToken)
            let all = chunks |> Array.concat |> utf8.GetString
            return all.Split('\n') |> Array.filter (fun l -> l <> "") |> List.ofArray
        }

    member this.CopyOutText(copySql: string) =
        this.CopyOutTextAsync(copySql).GetAwaiter().GetResult()

    // ---- Cancellation --------------------------------------------------------

    /// Cancel the query currently running on this connection's backend. Opens a
    /// *separate* short-lived socket and sends a CancelRequest (the server then
    /// aborts the in-flight query, which surfaces as a 57014 error there).
    member _.CancelAsync(?cancellationToken: CancellationToken) : Task =
        task {
            let ct = defaultArg cancellationToken CancellationToken.None
            use sock =
                match config.Endpoint with
                | Tcp _ -> new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
                | Unix _ -> new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified)
            match config.Endpoint with
            | Tcp(h, p) -> do! sock.ConnectAsync(h, p, ct)
            | Unix path -> do! sock.ConnectAsync(UnixDomainSocketEndPoint(path), ct)
            use stream = new NetworkStream(sock, ownsSocket = true)
            let bytes = (cancelRequest backendPid backendKey).ToBytes()
            do! stream.WriteAsync(System.ReadOnlyMemory(bytes), ct)
            do! stream.FlushAsync(ct)
        }

    member this.Cancel() = this.CancelAsync().GetAwaiter().GetResult()

    // ---- LISTEN / NOTIFY -----------------------------------------------------

    /// Raised whenever a NotificationResponse is observed during message reads.
    member _.NotificationReceived = notifyEvent.Publish

    /// Pull a notification already buffered from earlier message processing.
    member _.DequeueNotification() =
        if notifications.Count > 0 then Some(notifications.Dequeue()) else None

    member this.ListenAsync(channel: string, ?cancellationToken: CancellationToken) : Task =
        task {
            let! _ = this.QueryAsync(sprintf "LISTEN %s" channel, ?cancellationToken = cancellationToken)
            ()
        }

    member this.Listen(channel: string) = this.Query(sprintf "LISTEN %s" channel) |> ignore

    /// Wait up to `timeout` for an async notification, reading the socket while
    /// otherwise idle. Returns None on timeout.
    member _.WaitForNotificationAsync(timeout: TimeSpan, ?cancellationToken: CancellationToken) : Task<Notification option> =
        task {
            if notifications.Count > 0 then
                return Some(notifications.Dequeue())
            else
                let ct = defaultArg cancellationToken CancellationToken.None
                use cts = ConnUtil.linked ct timeout
                try
                    let mutable result : Notification option = None
                    let mutable looping = true
                    while looping do
                        let! m = transport.ReadMessageAsync(cts.Token)
                        match m.Tag with
                        | 'A' ->
                            let pid, ch, payload = parseNotification m
                            result <- Some { Pid = pid; Channel = ch; Payload = payload }
                            looping <- false
                        | 'S' -> // ParameterStatus
                            m.CString() |> ignore
                            m.CString() |> ignore
                        | 'N' -> () // NoticeResponse
                        | _ -> ()
                    return result
                with :? OperationCanceledException ->
                    return None
        }

    member this.WaitForNotification(timeout: TimeSpan) =
        this.WaitForNotificationAsync(timeout).GetAwaiter().GetResult()

    // ---- Legacy function-call protocol --------------------------------------

    /// Invoke a function directly by OID via the function-call protocol ('F').
    /// Arguments are sent in text format; returns the raw (nullable) result.
    member this.CallFunctionAsync
        (functionOid: int, args: byte[] option list, ?resultBinary: bool, ?cancellationToken: CancellationToken)
        : Task<byte[] option> =
        task {
            let rb = defaultArg resultBinary false
            let ct = defaultArg cancellationToken CancellationToken.None
            use cts = ConnUtil.linked ct this.CommandTimeout
            let tok = cts.Token
            do! transport.SendAsync(functionCall functionOid args rb, tok)
            let mutable result : byte[] option = None
            let mutable err : PostgresErrorFields option = None
            let mutable ready = false
            while not ready do
                let! m = transport.ReadMessageAsync(tok)
                match m.Tag with
                | 'V' -> result <- parseFunctionResult m
                | 'N' -> ()
                | 'E' -> err <- Some(ErrorFields.parse m)
                | 'Z' -> ready <- true
                | _ -> ()
            match err with
            | Some e -> return raise (PostgresException e)
            | None -> return result
        }

    // ---- Streaming replication ----------------------------------------------

    /// Send a Standby Status Update ('r' inside CopyData) acknowledging `lsn`.
    member private _.SendStandbyStatusAsync(lsn: int64, ct: CancellationToken) : Task =
        let m = OutgoingMessage('d')
        m.Byte(byte 'r')
        m.Int64(lsn) // last WAL byte received
        m.Int64(lsn) // last WAL byte flushed
        m.Int64(lsn) // last WAL byte applied
        m.Int64(0L) // client timestamp (0 acceptable)
        m.Byte(0uy) // reply requested? no
        transport.SendAsync(m, ct)

    /// Run a START_REPLICATION command and stream the WAL. `handler` is called
    /// with (walEnd, payload) for each XLogData message; return false from it to
    /// stop. Keepalives are answered with a standby status update. Works for
    /// both logical (test_decoding payloads are text) and physical streams.
    member this.StreamReplicationAsync
        (command: string, handler: int64 * byte[] -> bool, ?cancellationToken: CancellationToken)
        : Task =
        task {
            let ct = defaultArg cancellationToken CancellationToken.None
            do! transport.SendAsync(query dataEncoding command, ct)

            // Await CopyBothResponse 'W' (or an error).
            let mutable starting = true
            let mutable startErr : PostgresErrorFields option = None
            while starting do
                let! m = transport.ReadMessageAsync(ct)
                match m.Tag with
                | 'W' -> starting <- false
                | 'E' ->
                    startErr <- Some(ErrorFields.parse m)
                    starting <- false
                | _ -> ()
            match startErr with
            | Some e -> return raise (PostgresException e)
            | None -> ()

            // Stream CopyData until the handler stops or the server ends the COPY.
            let mutable lastLsn = 0L
            let mutable stopRequested = false
            let mutable streaming = true
            let mutable streamErr : PostgresErrorFields option = None
            while streaming do
                let! m = transport.ReadMessageAsync(ct)
                match m.Tag with
                | 'd' ->
                    let body = m.Body
                    match char body.[0] with
                    | 'w' -> // XLogData: 'w' walStart(8) walEnd(8) sendTime(8) data
                        let walEnd = BinaryPrimitives.ReadInt64BigEndian(ReadOnlySpan<byte>(body, 9, 8))
                        let data = body.[25..]
                        if walEnd > lastLsn then lastLsn <- walEnd
                        if not (handler (walEnd, data)) then
                            stopRequested <- true
                            streaming <- false
                    | 'k' -> // primary keepalive: 'k' walEnd(8) sendTime(8) replyRequested(1)
                        let walEnd = BinaryPrimitives.ReadInt64BigEndian(ReadOnlySpan<byte>(body, 1, 8))
                        if walEnd > lastLsn then lastLsn <- walEnd
                        if body.[17] <> 0uy then
                            do! this.SendStandbyStatusAsync(lastLsn, ct)
                    | _ -> ()
                | 'c' -> streaming <- false // server CopyDone
                | 'E' ->
                    streamErr <- Some(ErrorFields.parse m)
                    streaming <- false
                | _ -> ()

            match streamErr with
            | Some e -> return raise (PostgresException e)
            | None ->
                // Acknowledge and end the COPY; drain to ReadyForQuery.
                if stopRequested then
                    do! this.SendStandbyStatusAsync(lastLsn, ct)
                    do! transport.SendAsync(copyDone (), ct)
                let mutable ready = false
                while not ready do
                    let! m = transport.ReadMessageAsync(ct)
                    match m.Tag with
                    | 'Z' -> ready <- true
                    | 'E' -> return raise (PostgresException(ErrorFields.parse m))
                    | _ -> ()
        }

    // ---- Teardown ------------------------------------------------------------

    member _.CloseAsync() : Task =
        task {
            if not (isNull (box transport)) then
                try
                    do! transport.SendAsync(terminate (), CancellationToken.None)
                with _ -> ()
                transport.Dispose()
                transport <- Unchecked.defaultof<Transport>
            if not (isNull socket) then
                socket.Dispose()
                socket <- null
        }

    member this.Close() = this.CloseAsync().GetAwaiter().GetResult()

    interface IDisposable with
        member this.Dispose() = this.Close()

    interface IAsyncDisposable with
        member this.DisposeAsync() = ValueTask(this.CloseAsync())
