namespace Rbt.Postgres.Tests

open System
open System.Buffers.Binary
open System.Globalization
open System.Text
open Xunit
open Rbt.Postgres

[<Collection("postgres")>]
type CodecRegressionTests(fixture: PostgresFixture) =

    let decodeText oid (text: string) = Codecs.decode Encoding.UTF8 oid 0 (Encoding.UTF8.GetBytes text)
    let binaryInt64 n =
        let bytes = Array.zeroCreate<byte> 8
        BinaryPrimitives.WriteInt64BigEndian(Span<byte>(bytes), n)
        bytes

    [<Fact>]
    member _.``binary temporal microseconds cannot wrap ticks``() =
        for oid in [ Oids.timestamp; Oids.timestamptz ] do
            for micros in [ Int64.MaxValue - 1L; Int64.MinValue + 1L ] do
                Assert.Throws<OverflowException>(fun () -> Codecs.decode Encoding.UTF8 oid 1 (binaryInt64 micros) |> ignore)
                |> ignore
        let interval = Array.zeroCreate<byte> 16
        BinaryPrimitives.WriteInt64BigEndian(Span<byte>(interval, 0, 8), Int64.MaxValue)
        Assert.Throws<OverflowException>(fun () -> Codecs.decode Encoding.UTF8 Oids.interval 1 interval |> ignore)
        |> ignore

    [<Fact>]
    member _.``numeric rejects rounded decimal values and retains exact values``() =
        Assert.Equal(1.25m, decodeText Oids.numeric "1.2500" :?> decimal)
        Assert.Equal(0.0000000000000000000000000001m, decodeText Oids.numeric "0.0000000000000000000000000001" :?> decimal)
        for input in [ "0.00000000000000000000000000001"; "1.00000000000000000000000000001" ] do
            Assert.Throws<OverflowException>(fun () -> decodeText Oids.numeric input |> ignore) |> ignore
        Assert.Throws<OverflowException>(fun () -> decodeText Oids.numericArray "{1.00000000000000000000000000001}" |> ignore)
        |> ignore

    [<Fact>]
    member _.``array dimension prefixes preserve nested elements and nulls``() =
        let rows = decodeText Oids.int4Array "[0:1][-2:-1]={{1,NULL},{3,4}}" :?> obj[]
        Assert.Equal(2, rows.Length)
        let first = rows.[0] :?> obj[]
        let second = rows.[1] :?> obj[]
        Assert.Equal(1, first.[0] :?> int)
        Assert.Null(first.[1])
        Assert.Equal(3, second.[0] :?> int)
        Assert.Equal(4, second.[1] :?> int)
        let strings = decodeText Oids.textArray "[5:6]={\"a,b\",\"NULL\"}" :?> obj[]
        Assert.Equal("a,b", strings.[0] :?> string)
        Assert.Equal("NULL", strings.[1] :?> string)

    [<Fact>]
    member _.``legacy bytea escape decoding handles octal and escaped backslashes``() =
        let bytes = decodeText Oids.bytea @"\000\\\377A" :?> byte[]
        Assert.Equal<byte>([| 0uy; 92uy; 255uy; 65uy |], bytes)
        Assert.Equal<byte>([| 0uy; 255uy |], decodeText Oids.bytea @"\x00FF" :?> byte[])
        let array = decodeText Oids.byteaArray "{\"\\\\001\\\\\\\\\"}" :?> obj[]
        Assert.Equal<byte>([| 1uy; 92uy |], array.[0] :?> byte[])
        Assert.Throws<ArgumentException>(fun () -> decodeText Oids.bytea @"\08A" |> ignore) |> ignore

    [<Fact>]
    member _.``binary number parameters parse strings invariantly``() =
        let original = CultureInfo.CurrentCulture
        try
            CultureInfo.CurrentCulture <- CultureInfo.GetCultureInfo("fr-FR")
            for oid, input in [ Oids.int2, "123"; Oids.int4, "123"; Oids.int8, "123"; Oids.oid, "123"; Oids.float4, "1.5"; Oids.float8, "1.5" ] do
                let format, _ = Codecs.encodeParam Encoding.UTF8 oid (box input)
                Assert.Equal(1, format)
            let _, doubleBytes = Codecs.encodeParam Encoding.UTF8 Oids.float8 (box "1.5")
            Assert.Equal(1.5, BinaryPrimitives.ReadDoubleBigEndian(ReadOnlySpan<byte>(doubleBytes)))
        finally
            CultureInfo.CurrentCulture <- original

    [<Fact>]
    member _.``time endpoint decodes without wrapping and encodes identically``() =
        let midnight = TimeSpan.FromDays(1.0)
        Assert.Equal(midnight, decodeText Oids.time "24:00:00" :?> TimeSpan)
        Assert.Equal(midnight, decodeText Oids.time "24:00:00.000000" :?> TimeSpan)
        Assert.Throws<ArgumentException>(fun () -> decodeText Oids.time "24:00:00.000001" |> ignore) |> ignore
        Assert.Equal(midnight, Codecs.decode Encoding.UTF8 Oids.time 1 (binaryInt64 86400000000L) :?> TimeSpan)
        let fmt, bytes = Codecs.encodeParam Encoding.UTF8 Oids.time (box midnight)
        Assert.Equal(1, fmt)
        Assert.Equal(86400000000L, BinaryPrimitives.ReadInt64BigEndian(ReadOnlySpan<byte>(bytes)))
        let ordinary = TimeSpan(12, 34, 56).Add(TimeSpan.FromTicks(1234560L))
        Assert.Equal(ordinary, decodeText Oids.time "12:34:56.123456" :?> TimeSpan)
        let _, encoded = Codecs.encodeParam Encoding.UTF8 Oids.time (box ordinary)
        Assert.Equal(ordinary, Codecs.decode Encoding.UTF8 Oids.time 1 encoded :?> TimeSpan)
        let _, timeOnly = Codecs.encodeParam Encoding.UTF8 Oids.time (box (TimeOnly(12, 34, 56)))
        Assert.Equal(TimeSpan(12, 34, 56), Codecs.decode Encoding.UTF8 Oids.time 1 timeOnly :?> TimeSpan)
        Assert.Throws<ArgumentException>(fun () -> Codecs.encodeParam Encoding.UTF8 Oids.time (box (midnight.Add(TimeSpan.FromTicks(10L)))) |> ignore)
        |> ignore
        let times = decodeText Oids.timeArray "[0:1]={24:00:00,12:34:56.123456}" :?> obj[]
        Assert.Equal(midnight, times.[0] :?> TimeSpan)
        Assert.Equal(ordinary, times.[1] :?> TimeSpan)
        let arrayFormat, arrayBytes = Codecs.encodeParam Encoding.UTF8 Oids.timeArray (box [| midnight; ordinary |])
        Assert.Equal(0, arrayFormat)
        let encodedTimes = Codecs.decode Encoding.UTF8 Oids.timeArray 0 arrayBytes :?> obj[]
        Assert.Equal(midnight, encodedTimes.[0] :?> TimeSpan)
        Assert.Equal(ordinary, encodedTimes.[1] :?> TimeSpan)
        let _, nestedBytes = Codecs.encodeParam Encoding.UTF8 Oids.timeArray (box [| [| box midnight; null |]; [| box ordinary; box midnight |] |])
        let nested = Codecs.decode Encoding.UTF8 Oids.timeArray 0 nestedBytes :?> obj[]
        let first = nested.[0] :?> obj[]
        Assert.Equal(midnight, first.[0] :?> TimeSpan)
        Assert.Null(first.[1])
        Assert.Equal(ordinary, (nested.[1] :?> obj[]).[0] :?> TimeSpan)

    [<Fact>]
    member _.``time endpoint and array round trip through server``() =
        fixture.Require()
        use conn = fixture.Connect()
        let row = conn.QueryTyped("SELECT TIME '24:00:00' AS endpoint, ARRAY[TIME '24:00:00', TIME '12:34:56.123456'] AS times").Rows |> List.exactlyOne
        Assert.Equal(TimeSpan.FromDays(1.0), row.Get<TimeSpan>(0))
        let times = row.Get<obj[]>(1)
        Assert.Equal(TimeSpan.FromDays(1.0), times.[0] :?> TimeSpan)
        Assert.Equal(TimeSpan(12, 34, 56).Add(TimeSpan.FromTicks(1234560L)), times.[1] :?> TimeSpan)
        let stmt = conn.Prepare("SELECT $1::time AS t")
        let result = conn.ExecutePrepared(stmt, [ Some(box (TimeSpan.FromDays(1.0))) ])
        Assert.Equal(TimeSpan.FromDays(1.0), (result.Rows |> List.exactlyOne).Get<TimeSpan>(0))
        let arrayStmt = conn.Prepare("SELECT $1::time[] AS times")
        let arrayResult = conn.ExecutePrepared(arrayStmt, [ Some(box [| TimeSpan.FromDays(1.0); TimeSpan(12, 34, 56) |]) ])
        let returned = (arrayResult.Rows |> List.exactlyOne).Get<obj[]>(0)
        Assert.Equal(TimeSpan.FromDays(1.0), returned.[0] :?> TimeSpan)
        Assert.Equal(TimeSpan(12, 34, 56), returned.[1] :?> TimeSpan)
