namespace Rbt.Postgres.Tests

open System
open System.IO
open System.Net
open System.Net.Security
open System.Net.Sockets
open System.Security.Cryptography
open System.Security.Cryptography.X509Certificates
open System.Text
open System.Threading
open Xunit
open Rbt.Postgres

module private SecurityTestHelpers =
    let proof password =
        let state : Scram.ScramState =
            { Password = password; ClientNonce = "nonce"; ClientFirstBare = "n=,r=nonce"; Binding = Scram.CbNone }
        Scram.finish state (Encoding.UTF8.GetBytes("r=nonce-server,s=c2FsdA==,i=4096"))

    let root name =
        use key = RSA.Create(2048)
        let req = CertificateRequest("CN=" + name, key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1)
        req.CertificateExtensions.Add(X509BasicConstraintsExtension(true, false, 0, true))
        req.CertificateExtensions.Add(X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign, true))
        req.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1))

    let leaf (ca: X509Certificate2) (eku: string) =
        use key = RSA.Create(2048)
        let req = CertificateRequest("CN=localhost", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1)
        let usages = OidCollection()
        usages.Add(Oid(eku)) |> ignore
        req.CertificateExtensions.Add(X509EnhancedKeyUsageExtension(usages, true))
        use cert = req.Create(ca, DateTimeOffset.UtcNow.AddHours(-1), DateTimeOffset.UtcNow.AddHours(1), RandomNumberGenerator.GetBytes(16))
        cert.CopyWithPrivateKey(key)

    let negotiate (cert: X509Certificate2) (rootPem: string) =
        let path = Path.GetTempFileName()
        File.WriteAllText(path, rootPem)
        use timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10.0))
        use listener = new TcpListener(IPAddress.Loopback, 0)
        listener.Start()
        let port = (listener.LocalEndpoint :?> IPEndPoint).Port
        let server = task {
            try
                use! client = listener.AcceptTcpClientAsync(timeout.Token)
                use stream = client.GetStream()
                let request = Array.zeroCreate<byte> 8
                do! stream.ReadExactlyAsync(request.AsMemory(), timeout.Token)
                do! stream.WriteAsync(ReadOnlyMemory([| byte 'S' |]), timeout.Token)
                use ssl = new SslStream(stream, false)
                let options = SslServerAuthenticationOptions(ServerCertificate = cert)
                do! ssl.AuthenticateAsServerAsync(options, timeout.Token)
            with _ -> ()
        }
        try
            use client = new TcpClient()
            client.ConnectAsync(IPAddress.Loopback, port, timeout.Token).GetAwaiter().GetResult()
            let cfg = { SslConfig.defaultFor "localhost" with Mode = VerifyCa; RootCertPath = Some path }
            try
                let result = Tls.negotiateAsync (client.GetStream()) cfg timeout.Token |> fun t -> t.GetAwaiter().GetResult()
                result.Stream.Dispose()
                true
            with _ -> false
        finally
            timeout.Cancel()
            server.GetAwaiter().GetResult()
            File.Delete(path)

type SecurityRegressionTests() =
    [<Fact>]
    member _.``SASLprep maps and normalizes passwords according to Unicode 3.2``() =
        Assert.Equal(SecurityTestHelpers.proof "IX", SecurityTestHelpers.proof "I\u00ADX")
        Assert.Equal(SecurityTestHelpers.proof "a b", SecurityTestHelpers.proof "a\u00A0b")
        Assert.Equal(SecurityTestHelpers.proof "IX", SecurityTestHelpers.proof "\u2168")
        Assert.Equal(SecurityTestHelpers.proof "\U0002136A", SecurityTestHelpers.proof "\U0002F868")

    [<Fact>]
    member _.``SASLprep falls back to raw password for prohibited unassigned and bidi input``() =
        // Each value contains a mappable character; equality to its unprepared
        // form demonstrates that PostgreSQL's all-or-nothing fallback was used.
        for raw in [ "I\u00AD\u0007"; "I\u00AD\u0221"; "a\u00AD\u0627" ] do
            let withoutMapped = raw.Replace("\u00AD", "")
            Assert.NotEqual(SecurityTestHelpers.proof withoutMapped, SecurityTestHelpers.proof raw)

    [<Fact>]
    member _.``SCRAM rejects excessive and invalid server iteration counts``() =
        let state, _ = Scram.start "secret" Scram.CbNone
        for count in [ "0"; "-1"; "1000001"; "2147483647" ] do
            let message = Encoding.UTF8.GetBytes($"r={state.ClientNonce}-server,s=c2FsdA==,i={count}")
            Assert.ThrowsAny<exn>(fun () -> Scram.finish state message |> ignore) |> ignore

    [<Fact>]
    member _.``sslmode allow starts with plaintext without writing SSLRequest``() =
        use stream = new MemoryStream()
        let cfg = { SslConfig.defaultFor "localhost" with Mode = Allow }
        let result = Tls.negotiateAsync stream cfg CancellationToken.None |> fun t -> t.GetAwaiter().GetResult()
        Assert.False(result.Encrypted)
        Assert.Equal(0L, stream.Length)

    [<Fact>]
    member _.``custom CA bundle considers every root and enforces server authentication EKU``() =
        use unrelated = SecurityTestHelpers.root "unrelated"
        use ca = SecurityTestHelpers.root "trusted"
        use server = SecurityTestHelpers.leaf ca "1.3.6.1.5.5.7.3.1"
        use clientOnly = SecurityTestHelpers.leaf ca "1.3.6.1.5.5.7.3.2"
        let bundle = unrelated.ExportCertificatePem() + "\n" + ca.ExportCertificatePem()
        Assert.True(SecurityTestHelpers.negotiate server bundle)
        Assert.False(SecurityTestHelpers.negotiate clientOnly bundle)
