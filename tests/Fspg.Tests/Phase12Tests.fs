namespace Fspg.Tests

open System
open System.Threading
open System.Threading.Tasks
open Xunit
open Fspg

[<Collection("postgres")>]
type Phase12Tests(fixture: PostgresFixture) =

    // ---- #2 graceful cancellation: connection stays usable ------------------

    [<Fact>]
    member _.``a cancelled query leaves the connection usable``() : Task =
        fixture.Require()
        task {
            use conn = fixture.Connect()
            use cts = new CancellationTokenSource(400)
            let mutable cancelled = false
            try
                let! _ = conn.QueryAsync("SELECT pg_sleep(30)", cts.Token)
                ()
            with :? OperationCanceledException ->
                cancelled <- true
            Assert.True(cancelled)
            // The key fix: the connection was drained to ReadyForQuery via a
            // server CancelRequest, so it is still usable.
            let rs = conn.Query("SELECT 1 AS x") |> List.exactlyOne
            Assert.Equal(Some "1", rs.Rows |> List.head |> Array.head)
        }

    // ---- #3 multi-dimensional arrays ----------------------------------------

    [<Fact>]
    member _.``multi-dimensional arrays decode to nested arrays``() =
        fixture.Require()
        use conn = fixture.Connect()
        let r = conn.QueryTyped("SELECT '{{1,2},{3,4}}'::int4[] AS m")
        let m = (r.Rows |> List.head).Get<obj[]>(0)
        Assert.Equal(2, m.Length)
        let row0 = m.[0] :?> obj[]
        let row1 = m.[1] :?> obj[]
        Assert.Equal(1, row0.[0] :?> int)
        Assert.Equal(2, row0.[1] :?> int)
        Assert.Equal(3, row1.[0] :?> int)
        Assert.Equal(4, row1.[1] :?> int)

    // ---- #4 range types -----------------------------------------------------

    [<Fact>]
    member _.``range types decode to a typed PgRange``() =
        fixture.Require()
        use conn = fixture.Connect()
        let r =
            conn.QueryTyped(
                "SELECT '[1,10)'::int4range AS r, '[2026-01-01,2026-12-31]'::daterange AS d, 'empty'::int4range AS e")
        let row = r.Rows |> List.head
        let rng = row.Get<PgRange>(row.GetOrdinal "r")
        Assert.Equal(1, rng.Lower.Value :?> int)
        Assert.Equal(10, rng.Upper.Value :?> int)
        Assert.True(rng.LowerInclusive)
        Assert.False(rng.UpperInclusive)
        let dr = row.Get<PgRange>(row.GetOrdinal "d")
        Assert.Equal(DateOnly(2026, 1, 1), dr.Lower.Value :?> DateOnly)
        Assert.True((row.Get<PgRange>(row.GetOrdinal "e")).IsEmpty)

    // ---- #4 escape hatch: custom type codec ---------------------------------

    [<Fact>]
    member _.``a custom codec can be registered for any type (inet -> IPAddress)``() =
        fixture.Require()
        use conn = fixture.Connect()
        // inet (oid 869) normally decodes to its text form. Register a codec.
        let inetOid = 869
        Codecs.registerText inetOid (fun s -> box (System.Net.IPAddress.Parse(s.Split('/').[0])))
        try
            let r = conn.QueryTyped("SELECT '192.168.1.5'::inet AS ip")
            let ip = (r.Rows |> List.head).Get<System.Net.IPAddress>(0)
            Assert.Equal(System.Net.IPAddress.Parse "192.168.1.5", ip)
        finally
            Codecs.unregister inetOid // don't leak global state to other tests

    // ---- #5 connection liveness check ---------------------------------------

    [<Fact>]
    member _.``IsHealthy reflects whether the connection is alive``() : Task =
        fixture.Require()
        task {
            let conn = fixture.Connect()
            Assert.True(conn.IsHealthy)
            // Terminate this backend from a separate connection.
            use killer = fixture.Connect()
            killer.Query(sprintf "SELECT pg_terminate_backend(%d)" conn.BackendProcessId) |> ignore
            do! Task.Delay 300 // let the FIN / FATAL arrive
            Assert.False(conn.IsHealthy)
            conn.Close()
        }
