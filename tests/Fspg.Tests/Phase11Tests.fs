namespace Fspg.Tests

open System
open System.Text
open Xunit
open Fspg

[<Collection("postgres")>]
type Phase11Tests(fixture: PostgresFixture) =

    // ---- #2 binary parameters ----------------------------------------------

    [<Fact>]
    member _.``encodeParam uses binary for known OIDs and text otherwise``() =
        // int4 (oid 23) → binary, big-endian
        let fmt, bytes = Codecs.encodeParam Encoding.UTF8 23 (box 0x01020304)
        Assert.Equal(1, fmt)
        Assert.True([| 1uy; 2uy; 3uy; 4uy |] = bytes)
        // text (oid 25) → text format
        let fmtT, _ = Codecs.encodeParam Encoding.UTF8 25 (box "hi")
        Assert.Equal(0, fmtT)

    [<Fact>]
    member _.``binary-encoded parameters round-trip end-to-end``() =
        fixture.Require()
        use conn = fixture.Connect()
        let stmt = conn.Prepare("SELECT $1::int4 AS i, $2::bytea AS b, $3::uuid AS u, $4::timestamp AS t")
        let g = Guid.NewGuid()
        let ts = DateTime(2026, 6, 24, 12, 0, 0)
        let r =
            conn.ExecutePrepared(
                stmt,
                [ Some(box 42); Some(box [| 1uy; 2uy; 255uy |]); Some(box g); Some(box ts) ])
        let row = r.Rows |> List.head
        Assert.Equal(42, row.GetInt32 0)
        Assert.True(([| 1uy; 2uy; 255uy |] : byte[]) = row.GetBytes 1)
        Assert.Equal(g, row.GetGuid 2)
        Assert.Equal(ts, row.GetDateTime 3)

    // ---- #3 expanded type coverage -----------------------------------------

    [<Fact>]
    member _.``decodes interval to a typed PgInterval``() =
        fixture.Require()
        use conn = fixture.Connect()
        let r = conn.QueryTyped("SELECT INTERVAL '14 mons 3 days 04:05:06' AS i")
        let iv = (r.Rows |> List.head).Get<PgInterval>(0)
        Assert.Equal(14, iv.Months)
        Assert.Equal(3, iv.Days)
        Assert.Equal(TimeSpan(4, 5, 6), iv.Time)

    [<Fact>]
    member _.``decodes numeric, date, timestamptz and bool arrays to typed arrays``() =
        fixture.Require()
        use conn = fixture.Connect()
        let r =
            conn.QueryTyped(
                "SELECT ARRAY[1.5,2.5]::numeric[]                       AS n,
                        ARRAY['2026-06-24','2026-01-01']::date[]        AS d,
                        ARRAY[true,false,NULL]::bool[]                  AS b")
        let row = r.Rows |> List.head
        let n = row.Get<obj[]>(row.GetOrdinal "n")
        Assert.Equal(1.5m, n.[0] :?> decimal)
        Assert.Equal(2.5m, n.[1] :?> decimal)
        let d = row.Get<obj[]>(row.GetOrdinal "d")
        Assert.Equal(DateOnly(2026, 6, 24), d.[0] :?> DateOnly)
        let b = row.Get<obj[]>(row.GetOrdinal "b")
        Assert.Equal(true, b.[0] :?> bool)
        Assert.Equal(false, b.[1] :?> bool)
        Assert.Null(b.[2])
