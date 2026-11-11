namespace Rbt.Postgres.Tests

open System
open System.Threading
open System.Threading.Tasks
open Xunit
open Rbt.Postgres

module private ConnectionTestServer =
    open System.Buffers.Binary
    open System.IO
    open System.Net
    open System.Net.Sockets
    open System.Text

    let readBytes (stream: Stream) count (ct: CancellationToken) =
        task {
            let bytes = Array.zeroCreate<byte> count
            do! stream.ReadExactlyAsync(bytes.AsMemory(), ct)
            return bytes
        }

    let readStartup (stream: Stream) ct =
        task {
            let! header = readBytes stream 4 ct
            let length = BinaryPrimitives.ReadInt32BigEndian(header.AsSpan())
            let! _ = readBytes stream (length - 4) ct
            return ()
        }

    let readMessage (stream: Stream) ct =
        task {
            let! header = readBytes stream 5 ct
            let length = BinaryPrimitives.ReadInt32BigEndian(ReadOnlySpan<byte>(header, 1, 4))
            let! body = readBytes stream (length - 4) ct
            return char header.[0], body
        }

    let send (stream: Stream) tag (body: byte[]) ct =
        task {
            let bytes = Array.zeroCreate<byte> (body.Length + 5)
            bytes.[0] <- byte tag
            BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(1, 4), body.Length + 4)
            Array.Copy(body, 0, bytes, 5, body.Length)
            do! stream.WriteAsync(bytes.AsMemory(), ct)
        }

    let startupOk stream ct =
        task {
            do! send stream 'R' [| 0uy; 0uy; 0uy; 0uy |] ct
            do! send stream 'Z' [| byte 'I' |] ct
        }

    let config port =
        { Endpoint = Tcp("127.0.0.1", port)
          User = "tester"
          Password = "secret"
          Database = "testdb" }

    let putInt64 (bytes: byte[]) offset value =
        BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan(offset, 8), value)

[<Collection("postgres")>]
type ConnectionRegressionTests(fixture: PostgresFixture) =
    [<Fact>]
    member _.``typed parameter status is applied without losing the next result``() =
        fixture.Require()
        use conn = fixture.Connect()
        conn.QueryTyped("SET application_name = 'rbt_typed_status'") |> ignore
        Assert.Equal("rbt_typed_status", conn.ServerParameters.["application_name"])
        let result = conn.Query("SELECT 42") |> List.exactlyOne
        Assert.Equal(Some "42", result.Rows.Head.[0])

    [<Fact>]
    member _.``prepared SET processes parameter status``() =
        fixture.Require()
        use conn = fixture.Connect()
        let stmt = conn.Prepare("SET application_name = 'rbt_prepared_status'")
        conn.ExecutePrepared(stmt, []) |> ignore
        Assert.Equal("rbt_prepared_status", conn.ServerParameters.["application_name"])
        let result = conn.Query("SELECT 42") |> List.exactlyOne
        Assert.Equal(Some "42", result.Rows.Head.[0])

    [<Fact>]
    member _.``replication startup error is drained``() : Task =
        fixture.Require()
        task {
            use conn = fixture.NewConnection()
            conn.ReplicationMode <- Some "database"
            conn.Open()
            let mutable failed = false
            try
                do! conn.StreamReplicationAsync("START_REPLICATION SLOT rbt_missing_regression_slot LOGICAL 0/0", fun _ -> false)
            with :? PostgresException -> failed <- true
            Assert.True(failed)
            let identity = Replication.identifySystem conn
            Assert.NotEmpty(identity.SystemId)
        }

    [<Fact>]
    member _.``decoder failure cannot leave stale results on a usable connection``() =
        fixture.Require()
        use conn = fixture.Connect()
        Codecs.registerText 869 (fun _ -> failwith "decoder failed")
        try
            Assert.ThrowsAny<Exception>(fun () -> conn.QueryTyped("SELECT '127.0.0.1'::inet UNION ALL SELECT '127.0.0.2'::inet") |> ignore) |> ignore
            Assert.False(conn.IsHealthy)
            Assert.ThrowsAny<Exception>(fun () -> conn.Query("SELECT 42") |> ignore) |> ignore
        finally
            Codecs.unregister 869

    [<Fact>]
    member _.``failed lazy COPY input cannot leave a reusable stale response``() : Task =
        fixture.Require()
        task {
            use conn = fixture.Connect()
            conn.Query("CREATE TEMP TABLE copy_regression (x int)") |> ignore
            let chunks = seq { yield System.Text.Encoding.UTF8.GetBytes("1\n"); failwith "input failed" }
            let mutable failed = false
            try
                let! _ = conn.CopyInRawAsync("COPY copy_regression FROM STDIN", chunks)
                ()
            with _ -> failed <- true
            Assert.True(failed)
            Assert.False(conn.IsHealthy)
            Assert.ThrowsAny<Exception>(fun () -> conn.Query("SELECT 42") |> ignore) |> ignore
        }

    [<Fact>]
    member _.``pre-cancelled query does not execute``() : Task =
        fixture.Require()
        task {
            use conn = fixture.Connect()
            conn.Query("CREATE TEMP TABLE cancel_regression (x int)") |> ignore
            use cts = new CancellationTokenSource()
            cts.Cancel()
            try
                let! _ = conn.QueryAsync("INSERT INTO cancel_regression VALUES (1)", cts.Token)
                failwith "expected cancellation"
            with :? OperationCanceledException -> ()
            let result = conn.Query("SELECT count(*) FROM cancel_regression") |> List.exactlyOne
            Assert.Equal(Some "0", result.Rows.Head.[0])
        }

    [<Fact>]
    member _.``enumerator cancellation is honored``() : Task =
        fixture.Require()
        task {
            use conn = fixture.Connect()
            use cts = new CancellationTokenSource()
            cts.Cancel()
            let e = conn.Stream("SELECT 1").GetAsyncEnumerator(cts.Token)
            let mutable cancelled = false
            try
                let! _ = e.MoveNextAsync()
                ()
            with :? OperationCanceledException -> cancelled <- true
            do! e.DisposeAsync()
            Assert.True(cancelled)
        }

    [<Fact>]
    member _.``cancellation discards already buffered stream rows and invalidates``() : Task =
        fixture.Require()
        task {
            use conn = fixture.Connect()
            use cts = new CancellationTokenSource()
            let e = conn.Stream("SELECT generate_series(1, 20)", batchSize = 10).GetAsyncEnumerator(cts.Token)
            let! first = e.MoveNextAsync()
            Assert.True(first)
            Assert.Equal(1, e.Current.Get<int>(0))
            cts.Cancel()
            let mutable cancelled = false
            try
                let! _ = e.MoveNextAsync()
                ()
            with :? OperationCanceledException -> cancelled <- true
            do! e.DisposeAsync()
            Assert.True(cancelled)
            Assert.False(conn.IsHealthy)
            Assert.ThrowsAny<Exception>(fun () -> conn.Query("SELECT 42") |> ignore) |> ignore
        }

    [<Fact>]
    member _.``cancelling an active stream invalidates it and disposal is safe``() : Task =
        fixture.Require()
        task {
            use conn = fixture.Connect()
            use cts = new CancellationTokenSource()
            let e = conn.Stream("SELECT pg_sleep(10), 1").GetAsyncEnumerator(cts.Token)
            let pending = e.MoveNextAsync().AsTask()
            do! Task.Delay(200)
            cts.Cancel()
            let mutable cancelled = false
            try
                let! _ = pending
                ()
            with :? OperationCanceledException -> cancelled <- true
            do! e.DisposeAsync()
            Assert.True(cancelled)
            Assert.False(conn.IsHealthy)
            Assert.ThrowsAny<Exception>(fun () -> conn.Query("SELECT 42") |> ignore) |> ignore
        }

    [<Fact>]
    member _.``pgoutput distinguishes null toast text and binary``() =
        let payload = [| byte 'I'; 0uy; 0uy; 0uy; 1uy; byte 'N'; 0uy; 4uy; byte 'n'; byte 'u'; byte 't'; 0uy; 0uy; 0uy; 1uy; byte 'x'; byte 'b'; 0uy; 0uy; 0uy; 2uy; 255uy; 0uy |]
        match PgOutput.parse payload with
        | PgOutput.Insert(1, values) ->
            Assert.Equal(PgOutput.Null, values.[0])
            Assert.Equal(PgOutput.UnchangedToast, values.[1])
            Assert.Equal(PgOutput.Text "x", values.[2])
            Assert.Equal(PgOutput.Binary [| 255uy; 0uy |], values.[3])
        | other -> failwithf "unexpected message %A" other

type ConnectionProtocolRegressionTests() =
    [<Theory>]
    [<InlineData(false, 0L)>]
    [<InlineData(true, 77L)>]
    member _.``replication reports only explicit durable acknowledgement and answers server CopyDone``
        (acknowledge: bool, expected: int64) : Task =
        task {
            use timeout = new CancellationTokenSource(TimeSpan.FromSeconds 10.0)
            use listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0)
            listener.Start()
            let port = (listener.LocalEndpoint :?> System.Net.IPEndPoint).Port
            let server = task {
                use! client = listener.AcceptTcpClientAsync(timeout.Token)
                use stream = client.GetStream()
                do! ConnectionTestServer.readStartup stream timeout.Token
                do! ConnectionTestServer.startupOk stream timeout.Token
                let! (queryTag, _) = ConnectionTestServer.readMessage stream timeout.Token
                Assert.Equal('Q', queryTag)
                do! ConnectionTestServer.send stream 'W' [| 0uy; 0uy; 0uy |] timeout.Token
                let data = Array.zeroCreate<byte> 26
                data.[0] <- byte 'w'
                ConnectionTestServer.putInt64 data 1 50L
                ConnectionTestServer.putInt64 data 9 900L
                data.[25] <- 42uy
                do! ConnectionTestServer.send stream 'd' data timeout.Token
                let keepalive = Array.zeroCreate<byte> 18
                keepalive.[0] <- byte 'k'
                ConnectionTestServer.putInt64 keepalive 1 900L
                keepalive.[17] <- 1uy
                do! ConnectionTestServer.send stream 'd' keepalive timeout.Token
                let! (statusTag, status) = ConnectionTestServer.readMessage stream timeout.Token
                Assert.Equal('d', statusTag)
                Assert.Equal(byte 'r', status.[0])
                let get offset = System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(status.AsSpan(offset, 8))
                Assert.Equal(expected, get 1)
                Assert.Equal(expected, get 9)
                Assert.Equal(expected, get 17)
                do! ConnectionTestServer.send stream 'c' [||] timeout.Token
                let! (doneTag, _) = ConnectionTestServer.readMessage stream timeout.Token
                Assert.Equal('c', doneTag)
                do! ConnectionTestServer.send stream 'C' (System.Text.Encoding.UTF8.GetBytes("COPY 0\000")) timeout.Token
                do! ConnectionTestServer.send stream 'Z' [| byte 'I' |] timeout.Token
            }
            use conn = new Connection(ConnectionTestServer.config port)
            conn.SslMode <- Disable
            conn.ReplicationMode <- Some "database"
            do! conn.OpenAsync(timeout.Token)
            let mutable observed = false
            do! conn.StreamReplicationAsync("START_REPLICATION", (fun (walEnd, payload) ->
                Assert.Equal(900L, walEnd)
                Assert.Equal<byte>([| 42uy |], payload)
                observed <- true
                if acknowledge then conn.AcknowledgeReplication(77L)
                true), timeout.Token)
            do! server
            Assert.True(observed)
        }

    [<Fact>]
    member _.``sslmode allow retries localized authentication rejection over TLS``() : Task =
        task {
            use timeout = new CancellationTokenSource(TimeSpan.FromSeconds 10.0)
            use key = System.Security.Cryptography.RSA.Create(2048)
            let request =
                System.Security.Cryptography.X509Certificates.CertificateRequest(
                    "CN=localhost", key, System.Security.Cryptography.HashAlgorithmName.SHA256,
                    System.Security.Cryptography.RSASignaturePadding.Pkcs1)
            use cert = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1))
            use listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0)
            listener.Start()
            let port = (listener.LocalEndpoint :?> System.Net.IPEndPoint).Port
            let server = task {
                use! plaintext = listener.AcceptTcpClientAsync(timeout.Token)
                use first = plaintext.GetStream()
                do! ConnectionTestServer.readStartup first timeout.Token
                let rejection = System.Text.Encoding.UTF8.GetBytes("SFATAL\000C28000\000Mconnexion non chiffrée\000\000")
                do! ConnectionTestServer.send first 'E' rejection timeout.Token
                use! encrypted = listener.AcceptTcpClientAsync(timeout.Token)
                use raw = encrypted.GetStream()
                let! requestBytes = ConnectionTestServer.readBytes raw 8 timeout.Token
                Assert.Equal<byte>([| 0uy; 0uy; 0uy; 8uy; 4uy; 210uy; 22uy; 47uy |], requestBytes)
                do! raw.WriteAsync(ReadOnlyMemory([| byte 'S' |]), timeout.Token)
                use ssl = new System.Net.Security.SslStream(raw, false)
                let options = System.Net.Security.SslServerAuthenticationOptions(ServerCertificate = cert)
                do! ssl.AuthenticateAsServerAsync(options, timeout.Token)
                do! ConnectionTestServer.readStartup ssl timeout.Token
                do! ConnectionTestServer.startupOk ssl timeout.Token
            }
            use conn = new Connection(ConnectionTestServer.config port)
            conn.SslMode <- Allow
            do! conn.OpenAsync(timeout.Token)
            Assert.True(conn.IsTlsActive)
            do! server
        }
