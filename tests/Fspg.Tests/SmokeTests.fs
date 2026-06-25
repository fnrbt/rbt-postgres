namespace Fspg.Tests

open Xunit
open Fspg

[<Collection("postgres")>]
type SmokeTests(fixture: PostgresFixture) =

    [<Fact>]
    member _.``connects, authenticates, and runs SELECT 1``() =
        fixture.Require()
        use conn = fixture.Connect()
        let rs = conn.Query("SELECT 1 AS one") |> List.exactlyOne
        Assert.Equal("one", rs.Columns.[0].Name)
        let value = rs.Rows |> List.exactlyOne |> Array.exactlyOne
        Assert.Equal(Some "1", value)

    [<Fact>]
    member _.``reports the server version``() =
        fixture.Require()
        use conn = fixture.Connect()
        let rs = conn.Query("SELECT version()") |> List.exactlyOne
        let v = rs.Rows |> List.head |> Array.head |> Option.defaultValue ""
        Assert.StartsWith("PostgreSQL", v)
