namespace Fspg.Tests

open System.Threading.Tasks
open Xunit
open Fspg

[<Collection("postgres")>]
type Phase5Tests(fixture: PostgresFixture) =

    [<Fact>]
    member _.``a prepared statement is reusable, describes metadata, and closes``() =
        fixture.Require()
        use conn = fixture.Connect()
        let stmt = conn.Prepare("SELECT $1::int4 * 2 AS doubled")
        // Describe metadata
        Assert.True([| 23 |] = stmt.ParameterOids) // $1 is int4 (oid 23)
        Assert.Equal("doubled", stmt.Columns.[0].Name)
        // Reused across executes
        Assert.Equal(42, (conn.ExecutePrepared(stmt, [ Some(box 21) ]).Rows |> List.head).GetInt32 0)
        Assert.Equal(200, (conn.ExecutePrepared(stmt, [ Some(box 100) ]).Rows |> List.head).GetInt32 0)
        // Visible server-side as a named prepared statement
        let visible name =
            (conn.Query(sprintf "SELECT 1 FROM pg_prepared_statements WHERE name = '%s'" name)
             |> List.exactlyOne)
                .Rows.Length
        Assert.Equal(1, visible stmt.Name)
        // Close removes it
        conn.CloseStatement(stmt.Name)
        Assert.Equal(0, visible stmt.Name)

    [<Fact>]
    member _.``streams a large result in bounded batches``() : Task =
        fixture.Require()
        task {
            use conn = fixture.Connect()
            conn.Query("DROP TABLE IF EXISTS fspg_stream") |> ignore
            conn.Query("CREATE TABLE fspg_stream AS SELECT g AS id FROM generate_series(1,2500) g")
            |> ignore
            let mutable count = 0
            let mutable sum = 0L
            let e = conn.Stream("SELECT id FROM fspg_stream ORDER BY id", batchSize = 100).GetAsyncEnumerator()
            let mutable go = true
            while go do
                let! has = e.MoveNextAsync()
                if has then
                    count <- count + 1
                    sum <- sum + int64 (e.Current.GetInt32 0)
                else
                    go <- false
            do! e.DisposeAsync()
            Assert.Equal(2500, count)
            Assert.Equal(2500L * 2501L / 2L, sum)
            conn.Query("DROP TABLE IF EXISTS fspg_stream") |> ignore
        }

    [<Fact>]
    member _.``a partially consumed stream leaves the connection usable``() : Task =
        fixture.Require()
        task {
            use conn = fixture.Connect()
            conn.Query("DROP TABLE IF EXISTS fspg_stream2") |> ignore
            conn.Query("CREATE TABLE fspg_stream2 AS SELECT g FROM generate_series(1,500) g")
            |> ignore
            let e = conn.Stream("SELECT g FROM fspg_stream2 ORDER BY g", batchSize = 10).GetAsyncEnumerator()
            let! _ = e.MoveNextAsync() // read only the first row
            do! e.DisposeAsync() // abandon the rest
            // The connection must be clean and usable again.
            let rs = conn.Query("SELECT 1 AS x") |> List.exactlyOne
            Assert.Equal(Some "1", rs.Rows |> List.head |> Array.head)
            conn.Query("DROP TABLE IF EXISTS fspg_stream2") |> ignore
        }
