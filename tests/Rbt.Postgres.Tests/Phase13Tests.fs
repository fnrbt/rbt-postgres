namespace Rbt.Postgres.Tests

open System
open System.Threading
open System.Threading.Tasks
open Xunit
open Rbt.Postgres

[<Collection("postgres")>]
type Phase13Tests(fixture: PostgresFixture) =

    // ---- row-limited execute -------------------------------------------------

    [<Fact>]
    member _.``a row-limited execute stops at the limit and says rows were left``() =
        fixture.Require()
        use conn = fixture.Connect()
        let r = conn.ExecuteLimited("SELECT g FROM generate_series(1, 1000) g", [], 10)
        Assert.True(r.Truncated)
        Assert.Equal(10, r.Result.Rows.Length)
        Assert.Equal("g", r.Result.Columns.[0].Name)
        Assert.Equal(Some "10", r.Result.Rows |> List.last |> Array.head)
        // The portal was closed by Sync: the connection is ready for more.
        let again = conn.ExecuteLimited("SELECT 42", [], 10)
        Assert.False(again.Truncated)
        Assert.Equal(Some "42", again.Result.Rows |> List.exactlyOne |> Array.head)

    [<Fact>]
    member _.``a result within the limit is complete with its command tag``() =
        fixture.Require()
        use conn = fixture.Connect()
        let r = conn.ExecuteLimited("SELECT g FROM generate_series(1, 5) g", [], 5)
        Assert.False(r.Truncated)
        Assert.Equal(5, r.Result.Rows.Length)
        Assert.Equal("SELECT 5", r.Result.CommandTag)

    [<Fact>]
    member _.``a row-limited execute takes text parameters and reports non-row commands``() =
        fixture.Require()
        use conn = fixture.Connect()
        conn.Query("CREATE TEMP TABLE rbtpg_limited(x int)") |> ignore
        let ins = conn.ExecuteLimited("INSERT INTO rbtpg_limited SELECT g FROM generate_series(1, $1::int) g", [ Some "7" ], 3)
        Assert.False(ins.Truncated)
        Assert.Equal("INSERT 0 7", ins.Result.CommandTag)
        let sel = conn.ExecuteLimited("SELECT x FROM rbtpg_limited WHERE x > $1 ORDER BY x", [ Some "4" ], 100)
        Assert.Equal<string option list>([ Some "5"; Some "6"; Some "7" ], sel.Result.Rows |> List.map Array.head)

    [<Fact>]
    member _.``a row-limited execute refuses several statements``() =
        fixture.Require()
        use conn = fixture.Connect()
        let ex = Assert.Throws<PostgresException>(fun () -> conn.ExecuteLimited("SELECT 1; SELECT 2", [], 10) |> ignore)
        Assert.Equal("42601", ex.SqlState)
        Assert.Equal(Some "1", (conn.ExecuteLimited("SELECT 1", [], 1)).Result.Rows |> List.exactlyOne |> Array.head)

    [<Fact>]
    member _.``a row-limited execute times out and leaves the connection usable``() : Task =
        fixture.Require()
        task {
            use conn = fixture.Connect()
            conn.CommandTimeout <- TimeSpan.FromMilliseconds 300.0
            let! outcome =
                task {
                    try
                        let! _ = conn.ExecuteLimitedAsync("SELECT pg_sleep(30)", [], 10)
                        return "completed"
                    with :? TimeoutException -> return "timed out"
                }
            Assert.Equal("timed out", outcome)
            conn.CommandTimeout <- TimeSpan.FromSeconds 30.0
            let! r = conn.ExecuteLimitedAsync("SELECT 1", [], 1)
            Assert.Equal(Some "1", r.Result.Rows |> List.exactlyOne |> Array.head)
        }

    [<Fact>]
    member _.``a row-limited execute is cancelled by its token``() : Task =
        fixture.Require()
        task {
            use conn = fixture.Connect()
            use cts = new CancellationTokenSource(300)
            let! outcome =
                task {
                    try
                        let! _ = conn.ExecuteLimitedAsync("SELECT pg_sleep(30)", [], 10, cts.Token)
                        return "completed"
                    with :? OperationCanceledException -> return "cancelled"
                }
            Assert.Equal("cancelled", outcome)
        }

    // ---- notices ---------------------------------------------------------------

    [<Fact>]
    member _.``server notices arrive as events, not console output``() =
        fixture.Require()
        use conn = fixture.Connect()
        let seen = Collections.Generic.List<string>()
        conn.NoticeReceived.Add(fun notice -> seen.Add notice.MessageText)
        let stderr = Console.Error
        use captured = new IO.StringWriter()
        Console.SetError captured
        try
            conn.Query("DO $$ BEGIN RAISE NOTICE 'rbtpg notice %', 1; END $$") |> ignore
            conn.ExecuteLimited("DO $$ BEGIN RAISE WARNING 'rbtpg warning'; END $$", [], 1) |> ignore
        finally
            Console.SetError stderr
        Assert.Equal<string list>([ "rbtpg notice 1"; "rbtpg warning" ], List.ofSeq seen)
        Assert.Equal("", captured.ToString())
