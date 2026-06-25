namespace Fspg.Tests

open Xunit
open Fspg

[<Collection("postgres")>]
type Phase3Tests(fixture: PostgresFixture) =

    [<Fact>]
    member _.``over TLS the client uses SCRAM-SHA-256-PLUS (channel binding)``() =
        fixture.Require()
        Assert.True(TestConfig.isTcp fixture.Config, "needs TCP")
        use conn = fixture.NewConnection()
        conn.SslMode <- Require
        conn.Open() // success proves the cbind hash matched the real TLS cert
        Assert.Equal("SCRAM-SHA-256-PLUS", conn.AuthMechanism)

    [<Fact>]
    member _.``without TLS the client uses plain SCRAM-SHA-256``() =
        fixture.Require()
        Assert.True(TestConfig.isTcp fixture.Config, "needs TCP")
        use conn = fixture.NewConnection()
        conn.SslMode <- Disable
        conn.Open()
        Assert.Equal("SCRAM-SHA-256", conn.AuthMechanism)

    [<Fact>]
    member _.``a corrupted channel binding is rejected by the server``() =
        fixture.Require()
        Assert.True(TestConfig.isTcp fixture.Config, "needs TCP")
        use conn = fixture.NewConnection()
        conn.SslMode <- Require
        conn.TamperChannelBinding <- true
        // The server recomputes tls-server-end-point and must reject the mismatch.
        Assert.ThrowsAny<exn>(fun () -> conn.Open()) |> ignore
