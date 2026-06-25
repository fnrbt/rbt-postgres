namespace Fspg.Tests

open System
open Xunit
open Fspg

[<Collection("postgres")>]
type Phase4Tests(fixture: PostgresFixture) =

    [<Fact>]
    member _.``decodes scalar types to native .NET values``() =
        fixture.Require()
        use conn = fixture.Connect()
        let r =
            conn.QueryTyped(
                """
                SELECT 2147483647::int4              AS i4,
                       9223372036854775807::int8     AS i8,
                       (-12345)::int2                AS i2,
                       true                          AS b,
                       3.5::float4                   AS f4,
                       2.718281828::float8           AS f8,
                       12345.6789::numeric           AS num,
                       'héllo'::text                 AS t,
                       '\xDEADBEEF'::bytea           AS bytes,
                       '11111111-2222-3333-4444-555555555555'::uuid AS u,
                       '2026-06-24'::date            AS d,
                       '2026-06-24 12:34:56'::timestamp   AS ts,
                       '2026-06-24 12:34:56+00'::timestamptz AS tstz
                """)
        let row = r.Rows |> List.exactlyOne
        let o n = row.GetOrdinal n
        Assert.Equal(2147483647, row.GetInt32(o "i4"))
        Assert.Equal(9223372036854775807L, row.GetInt64(o "i8"))
        Assert.Equal(-12345s, row.GetInt16(o "i2"))
        Assert.True(row.GetBool(o "b"))
        Assert.Equal(3.5f, row.GetSingle(o "f4"))
        Assert.Equal(2.718281828, row.GetDouble(o "f8"), 9)
        Assert.Equal(12345.6789m, row.GetDecimal(o "num"))
        Assert.Equal("héllo", row.GetString(o "t"))
        Assert.True(([| 0xDEuy; 0xADuy; 0xBEuy; 0xEFuy |] : byte[]) = row.GetBytes(o "bytes"))
        Assert.Equal(Guid.Parse "11111111-2222-3333-4444-555555555555", row.GetGuid(o "u"))
        Assert.Equal(DateOnly(2026, 6, 24), row.Get<DateOnly>(o "d"))
        Assert.Equal(DateTime(2026, 6, 24, 12, 34, 56), row.GetDateTime(o "ts"))
        Assert.Equal(DateTimeOffset(2026, 6, 24, 12, 34, 56, TimeSpan.Zero), row.Get<DateTimeOffset>(o "tstz"))

    [<Fact>]
    member _.``requests binary for fixed-width types and text for numeric``() =
        fixture.Require()
        use conn = fixture.Connect()
        let r = conn.QueryTyped("SELECT 1::int4 AS i, 1.5::numeric AS n, 'x'::text AS t")
        let fmt name = (r.Columns |> Array.find (fun c -> c.Name = name)).Format
        Assert.Equal(1, fmt "i") // int4 decoded from binary
        Assert.Equal(0, fmt "n") // numeric decoded from text
        Assert.Equal(0, fmt "t") // text stays text

    [<Fact>]
    member _.``NULL decodes to None``() =
        fixture.Require()
        use conn = fixture.Connect()
        let r = conn.QueryTyped("SELECT NULL::int4 AS n, 5::int4 AS v")
        let row = r.Rows |> List.exactlyOne
        Assert.True(row.IsNull(row.GetOrdinal "n"))
        Assert.Equal(None, row.Raw(row.GetOrdinal "n"))
        Assert.Equal(5, row.GetInt32(row.GetOrdinal "v"))

    [<Fact>]
    member _.``decodes arrays via the generic text array codec``() =
        fixture.Require()
        use conn = fixture.Connect()
        let r = conn.QueryTyped("SELECT ARRAY[1,2,3]::int4[] AS a, ARRAY['x','y,z',NULL]::text[] AS t")
        let row = r.Rows |> List.exactlyOne
        let a = row.Get<obj[]>(row.GetOrdinal "a")
        Assert.Equal<int>([| 1; 2; 3 |], a |> Array.map (fun x -> x :?> int))
        let t = row.Get<obj[]>(row.GetOrdinal "t")
        Assert.Equal(3, t.Length)
        Assert.Equal("x", t.[0] :?> string)
        Assert.Equal("y,z", t.[1] :?> string)
        Assert.Null(t.[2])

    [<Fact>]
    member _.``typed parameters round-trip through the binary path``() =
        fixture.Require()
        use conn = fixture.Connect()
        let r =
            conn
                .QueryTypedAsync("SELECT $1::int4 + $2::int4 AS sum", [ Some(box 40); Some(box 2) ])
                .GetAwaiter()
                .GetResult()
        let row = r.Rows |> List.exactlyOne
        Assert.Equal(42, row.GetInt32 0)
