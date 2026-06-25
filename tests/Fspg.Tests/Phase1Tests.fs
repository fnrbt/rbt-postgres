namespace Fspg.Tests

open System
open System.Diagnostics
open System.Threading
open System.Threading.Tasks
open Xunit
open Fspg

[<Collection("postgres")>]
type Phase1Tests(fixture: PostgresFixture) =

    [<Fact>]
    member _.``QueryAsync returns rows over the async transport``() : Task =
        fixture.Require()
        task {
            use conn = fixture.Connect()
            let! results = conn.QueryAsync("SELECT 7 AS n")
            let rs = results |> List.exactlyOne
            Assert.Equal(Some "7", rs.Rows |> List.head |> Array.head)
        }

    [<Fact>]
    member _.``a cancellation token aborts a long-running query promptly``() =
        fixture.Require()
        use conn = fixture.Connect()
        use cts = new CancellationTokenSource(TimeSpan.FromMilliseconds 500.0)
        let sw = Stopwatch.StartNew()
        Assert.ThrowsAny<OperationCanceledException>(fun () ->
            conn.QueryAsync("SELECT pg_sleep(30)", cts.Token).GetAwaiter().GetResult()
            |> ignore)
        |> ignore
        sw.Stop()
        Assert.True(
            sw.Elapsed < TimeSpan.FromSeconds 10.0,
            sprintf "cancellation took too long: %A" sw.Elapsed)

    [<Fact>]
    member _.``a constraint violation surfaces SqlState and ConstraintName``() =
        fixture.Require()
        use conn = fixture.Connect()
        conn.Query("DROP TABLE IF EXISTS fspg_p1") |> ignore
        conn.Query("CREATE TABLE fspg_p1 (id int PRIMARY KEY)") |> ignore
        conn.Query("INSERT INTO fspg_p1 VALUES (1)") |> ignore
        let ex =
            Assert.Throws<PostgresException>(fun () ->
                conn.Query("INSERT INTO fspg_p1 VALUES (1)") |> ignore)
        Assert.Equal("23505", ex.SqlState) // unique_violation
        Assert.True(ex.ConstraintName.IsSome, "expected a constraint name")
        Assert.Equal("fspg_p1_pkey", ex.ConstraintName.Value)
        // Connection remains usable after an error.
        let rs = conn.Query("SELECT 'ok' AS s") |> List.exactlyOne
        Assert.Equal(Some "ok", rs.Rows |> List.head |> Array.head)
        conn.Query("DROP TABLE IF EXISTS fspg_p1") |> ignore
