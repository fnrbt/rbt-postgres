namespace Fspg.Tests

open Xunit
open Fspg

[<Collection("postgres")>]
type Phase2Tests(fixture: PostgresFixture) =

    let backendUsesSsl (conn: Connection) =
        let rs =
            conn.Query("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()")
            |> List.exactlyOne
        rs.Rows |> List.head |> Array.head

    [<Fact>]
    member _.``sslmode=require encrypts the connection``() =
        fixture.Require()
        Assert.True(TestConfig.isTcp fixture.Config, "TLS tests need a TCP endpoint")
        use conn = fixture.NewConnection()
        conn.SslMode <- Require
        conn.Open()
        Assert.True(conn.IsTlsActive, "expected TLS to be active")
        Assert.Equal(Some "t", backendUsesSsl conn)

    [<Fact>]
    member _.``sslmode=verify-full validates the server certificate against the CA``() =
        fixture.Require()
        Assert.True(TestConfig.isTcp fixture.Config, "TLS tests need a TCP endpoint")
        use conn = fixture.NewConnection()
        conn.SslMode <- VerifyFull
        conn.SslRootCert <- Some(TestConfig.rootCert ())
        conn.Open()
        Assert.True(conn.IsTlsActive)
        Assert.Equal(Some "t", backendUsesSsl conn)

    [<Fact>]
    member _.``sslmode=verify-full fails without the trusted CA``() =
        fixture.Require()
        Assert.True(TestConfig.isTcp fixture.Config, "TLS tests need a TCP endpoint")
        use conn = fixture.NewConnection()
        conn.SslMode <- VerifyFull
        conn.SslRootCert <- None // self-signed CA not trusted by the system store
        Assert.ThrowsAny<exn>(fun () -> conn.Open()) |> ignore

    [<Fact>]
    member _.``sslmode=disable stays in cleartext``() =
        fixture.Require()
        Assert.True(TestConfig.isTcp fixture.Config, "TLS tests need a TCP endpoint")
        use conn = fixture.NewConnection()
        conn.SslMode <- Disable
        conn.Open()
        Assert.False(conn.IsTlsActive)
        Assert.Equal(Some "f", backendUsesSsl conn)
