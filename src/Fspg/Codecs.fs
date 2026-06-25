namespace Fspg

open System
open System.Buffers.Binary
open System.Collections.Concurrent
open System.Globalization
open System.Text
open Fspg.Wire

/// Map a PostgreSQL client_encoding name to a .NET Encoding for the bytes↔string
/// step. Falls back to UTF-8 for names .NET can't resolve.
module PgEncoding =
    let resolve (pgName: string) : Encoding =
        match pgName.ToUpperInvariant().Replace("_", "").Replace("-", "") with
        | "UTF8" | "UNICODE" -> utf8
        | "LATIN1" | "ISO88591" -> Encoding.Latin1
        | "SQLASCII" -> Encoding.Latin1 // raw bytes; Latin1 is a lossless 1:1 map
        | "LATIN9" | "ISO885915" -> (try Encoding.GetEncoding "iso-8859-15" with _ -> Encoding.Latin1)
        | _ -> (try Encoding.GetEncoding(pgName) with _ -> utf8)

/// One decoded column within a typed row.
type PgColumn =
    { Name: string
      TypeOid: int
      /// The wire format actually used for this column (0 = text, 1 = binary).
      Format: int }

/// A typed row: column values already decoded to .NET objects (None = SQL NULL).
type PgRow(columns: PgColumn[], values: obj option []) =
    member _.Columns = columns
    member _.Values = values
    member _.IsNull(i: int) = values.[i].IsNone
    member _.Raw(i: int) = values.[i]
    member _.GetOrdinal(name: string) = columns |> Array.findIndex (fun c -> c.Name = name)

    member _.Value(i: int) : obj =
        match values.[i] with
        | Some v -> v
        | None -> null

    member this.Value(name: string) : obj = this.Value(this.GetOrdinal name)

    /// Strongly-typed accessor; throws on NULL or a type mismatch.
    member _.Get<'T>(i: int) : 'T =
        match values.[i] with
        | Some v -> unbox<'T> v
        | None -> failwithf "column %d is NULL" i

    member this.Get<'T>(name: string) : 'T = this.Get<'T>(this.GetOrdinal name)
    member this.GetInt16(i: int) = this.Get<int16>(i)
    member this.GetInt32(i: int) = this.Get<int>(i)
    member this.GetInt64(i: int) = this.Get<int64>(i)
    member this.GetBool(i: int) = this.Get<bool>(i)
    member this.GetSingle(i: int) = this.Get<float32>(i)
    member this.GetDouble(i: int) = this.Get<float>(i)
    member this.GetDecimal(i: int) = this.Get<decimal>(i)
    member this.GetString(i: int) = this.Get<string>(i)
    member this.GetGuid(i: int) = this.Get<Guid>(i)
    member this.GetBytes(i: int) = this.Get<byte[]>(i)
    member this.GetDateTime(i: int) = this.Get<DateTime>(i)

type TypedResult =
    { Columns: PgColumn[]
      Rows: PgRow list
      CommandTag: string }

/// A type codec: how to turn the wire bytes of one type into a .NET value, for
/// both the text and binary formats.
type Codec =
    { /// Whether to request this type in binary (true) or text (false).
      PreferBinary: bool
      DecodeText: string -> obj
      DecodeBinary: byte[] -> obj }

/// A PostgreSQL interval. Months and days are kept separate from the sub-day
/// time because they are not fixed-length (a month/day is calendar-relative).
type PgInterval =
    { Months: int
      Days: int
      Time: TimeSpan }

/// A PostgreSQL range value (e.g. int4range, daterange). A None bound is
/// unbounded (infinite); IsEmpty marks the empty range.
type PgRange =
    { Lower: obj option
      Upper: obj option
      LowerInclusive: bool
      UpperInclusive: bool
      IsEmpty: bool }

module Codecs =

    let private inv = CultureInfo.InvariantCulture
    let private span (b: byte[]) = ReadOnlySpan<byte>(b)

    /// User-registered codecs, consulted before the built-ins. The escape hatch
    /// for any type fspg doesn't decode natively (inet, hstore, composites, …).
    let private custom = ConcurrentDictionary<int, Codec>()

    // PostgreSQL stores temporal binary values relative to 2000-01-01.
    let private pgEpoch = DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Unspecified)
    let private pgEpochUtc = DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero)

    // ---- scalar decoders -----------------------------------------------------

    let private parseFloat (s: string) : float =
        match s with
        | "NaN" -> Double.NaN
        | "Infinity" -> Double.PositiveInfinity
        | "-Infinity" -> Double.NegativeInfinity
        | _ -> Double.Parse(s, NumberStyles.Float, inv)

    let private parseSingle (s: string) : float32 =
        match s with
        | "NaN" -> Single.NaN
        | "Infinity" -> Single.PositiveInfinity
        | "-Infinity" -> Single.NegativeInfinity
        | _ -> Single.Parse(s, NumberStyles.Float, inv)

    let private byteaText (s: string) : byte[] =
        if s.StartsWith("\\x") then Convert.FromHexString(s.Substring 2) else utf8.GetBytes s

    let private microsToTimeOnly (micros: int64) = TimeOnly(micros * 10L)
    let private daysToDate (days: int) = DateOnly(2000, 1, 1).AddDays(days)

    let private microsToTimestamp (micros: int64) =
        if micros = Int64.MaxValue then DateTime.MaxValue
        elif micros = Int64.MinValue then DateTime.MinValue
        else pgEpoch.AddTicks(micros * 10L)

    let private microsToTimestamptz (micros: int64) =
        if micros = Int64.MaxValue then DateTimeOffset.MaxValue
        elif micros = Int64.MinValue then DateTimeOffset.MinValue
        else pgEpochUtc.AddTicks(micros * 10L)

    // text/binary string codec (binary is just the UTF-8 bytes)
    let private stringCodec =
        { PreferBinary = false
          DecodeText = box
          DecodeBinary = fun b -> box (utf8.GetString b) }

    let private scalar preferBinary decodeText decodeBinary =
        { PreferBinary = preferBinary
          DecodeText = decodeText
          DecodeBinary = decodeBinary }

    // ---- array (text format, generic over an element codec) ------------------

    /// Recursively parse a PostgreSQL array literal into a (possibly nested,
    /// jagged) obj[]: "{1,2,3}" -> obj[]; "{{1,2},{3,4}}" -> obj[] of obj[].
    /// Elements are decoded with `decodeElem`; unquoted NULL becomes null.
    let private parseArrayText (decodeElem: string -> obj) (s: string) : obj =
        let mutable i = 0
        let n = s.Length
        let rec parseValue () : obj =
            if s.[i] = '{' then
                parseArray ()
            elif s.[i] = '"' then
                i <- i + 1
                let sb = Text.StringBuilder()
                while s.[i] <> '"' do
                    if s.[i] = '\\' then i <- i + 1
                    sb.Append(s.[i]) |> ignore
                    i <- i + 1
                i <- i + 1 // closing quote
                decodeElem (sb.ToString()) // a quoted element is never NULL
            else
                let start = i
                while i < n && s.[i] <> ',' && s.[i] <> '}' do
                    i <- i + 1
                let raw = s.Substring(start, i - start)
                if raw = "NULL" then null else decodeElem raw
        and parseArray () : obj =
            i <- i + 1 // consume '{'
            let items = ResizeArray<obj>()
            if i < n && s.[i] = '}' then
                i <- i + 1 // empty array
            else
                let mutable go = true
                while go do
                    items.Add(parseValue ())
                    if i < n && s.[i] = ',' then i <- i + 1
                    elif i < n && s.[i] = '}' then (i <- i + 1; go <- false)
                    else go <- false
            box (items.ToArray())
        parseArray ()

    let private arrayCodec (elementDecodeText: string -> obj) =
        { PreferBinary = false
          DecodeText = parseArrayText elementDecodeText
          DecodeBinary = fun b -> box (utf8.GetString b) }

    // ---- range (text format, generic over an element codec) ------------------

    let private parseRangeText (decodeElem: string -> obj) (s: string) : obj =
        if s = "empty" then
            box { Lower = None; Upper = None; LowerInclusive = false; UpperInclusive = false; IsEmpty = true }
        else
            let lowerInc = s.[0] = '['
            let upperInc = s.[s.Length - 1] = ']'
            let inner = s.Substring(1, s.Length - 2)
            let comma = inner.IndexOf(',') // a range literal has exactly one separating comma
            let bound (x: string) =
                let t = x.Trim('"')
                if t = "" then None else Some(decodeElem t)
            box
                { Lower = bound (inner.Substring(0, comma))
                  Upper = bound (inner.Substring(comma + 1))
                  LowerInclusive = lowerInc
                  UpperInclusive = upperInc
                  IsEmpty = false }

    let private rangeCodec (decodeElem: string -> obj) =
        { PreferBinary = false
          DecodeText = parseRangeText decodeElem
          DecodeBinary = fun b -> box (utf8.GetString b) }

    // ---- element text decoders (shared by scalar + array codecs) -------------

    let private dBool (s: string) = box (s = "t" || s = "true" || s = "1")
    let private dInt2 (s: string) = box (Int16.Parse(s, inv))
    let private dInt4 (s: string) = box (Int32.Parse(s, inv))
    let private dInt8 (s: string) = box (Int64.Parse(s, inv))
    let private dOid (s: string) = box (UInt32.Parse(s, inv))
    let private dFloat4 (s: string) = box (parseSingle s)
    let private dFloat8 (s: string) = box (parseFloat s)
    let private dNumeric (s: string) = box (Decimal.Parse(s, NumberStyles.Float, inv))
    let private dUuid (s: string) = box (Guid.Parse s)
    let private dDate (s: string) = box (DateOnly.ParseExact(s, "yyyy-MM-dd", inv))
    let private dTime (s: string) = box (TimeOnly.Parse(s, inv))
    let private dTimestamp (s: string) = box (DateTime.Parse(s, inv, DateTimeStyles.None))
    let private dTimestamptz (s: string) = box (DateTimeOffset.Parse(s, inv))
    let private dString (s: string) = box s
    let private dBytea (s: string) = box (byteaText s)

    // ---- binary decoders -----------------------------------------------------

    let private bInt2 (b: byte[]) = box (BinaryPrimitives.ReadInt16BigEndian(span b))
    let private bInt4 (b: byte[]) = box (BinaryPrimitives.ReadInt32BigEndian(span b))
    let private bInt8 (b: byte[]) = box (BinaryPrimitives.ReadInt64BigEndian(span b))
    let private bOid (b: byte[]) = box (BinaryPrimitives.ReadUInt32BigEndian(span b))
    let private bFloat4 (b: byte[]) = box (BinaryPrimitives.ReadSingleBigEndian(span b))
    let private bFloat8 (b: byte[]) = box (BinaryPrimitives.ReadDoubleBigEndian(span b))
    let private bUuid (b: byte[]) = box (Guid(span b, bigEndian = true))
    let private bDate (b: byte[]) = box (daysToDate (BinaryPrimitives.ReadInt32BigEndian(span b)))
    let private bTime (b: byte[]) = box (microsToTimeOnly (BinaryPrimitives.ReadInt64BigEndian(span b)))
    let private bTimestamp (b: byte[]) = box (microsToTimestamp (BinaryPrimitives.ReadInt64BigEndian(span b)))
    let private bTimestamptz (b: byte[]) = box (microsToTimestamptz (BinaryPrimitives.ReadInt64BigEndian(span b)))

    /// interval binary: micros(8) days(4) months(4).
    let private bInterval (b: byte[]) =
        let micros = BinaryPrimitives.ReadInt64BigEndian(ReadOnlySpan<byte>(b, 0, 8))
        let days = BinaryPrimitives.ReadInt32BigEndian(ReadOnlySpan<byte>(b, 8, 4))
        let months = BinaryPrimitives.ReadInt32BigEndian(ReadOnlySpan<byte>(b, 12, 4))
        box { Months = months; Days = days; Time = TimeSpan.FromTicks(micros * 10L) }

    // ---- registry ------------------------------------------------------------

    let private registry : Map<int, Codec> =
        [ // scalars (binary preferred where it is simple and exact)
          Oids.bool, scalar true dBool (fun b -> box (b.[0] <> 0uy))
          Oids.bytea, scalar true dBytea box
          Oids.int2, scalar true dInt2 bInt2
          Oids.int4, scalar true dInt4 bInt4
          Oids.int8, scalar true dInt8 bInt8
          Oids.oid, scalar true dOid bOid
          Oids.float4, scalar true dFloat4 bFloat4
          Oids.float8, scalar true dFloat8 bFloat8
          Oids.numeric, scalar false dNumeric box // text: lossless decimal
          Oids.uuid, scalar true dUuid bUuid
          Oids.date, scalar true dDate bDate
          Oids.time, scalar true dTime bTime
          Oids.timestamp, scalar true dTimestamp bTimestamp
          Oids.timestamptz, scalar true dTimestamptz bTimestamptz
          Oids.interval, scalar true dString bInterval
          Oids.text, stringCodec
          Oids.varchar, stringCodec
          Oids.name, stringCodec
          Oids.bpchar, stringCodec
          Oids.char, stringCodec
          Oids.json, stringCodec
          Oids.jsonb, stringCodec
          // arrays (text format) over the shared element text decoders
          Oids.boolArray, arrayCodec dBool
          Oids.byteaArray, arrayCodec dBytea
          Oids.charArray, arrayCodec dString
          Oids.nameArray, arrayCodec dString
          Oids.int2Array, arrayCodec dInt2
          Oids.int4Array, arrayCodec dInt4
          Oids.int8Array, arrayCodec dInt8
          Oids.oidArray, arrayCodec dOid
          Oids.float4Array, arrayCodec dFloat4
          Oids.float8Array, arrayCodec dFloat8
          Oids.numericArray, arrayCodec dNumeric
          Oids.uuidArray, arrayCodec dUuid
          Oids.dateArray, arrayCodec dDate
          Oids.timeArray, arrayCodec dTime
          Oids.timestampArray, arrayCodec dTimestamp
          Oids.timestamptzArray, arrayCodec dTimestamptz
          Oids.textArray, arrayCodec dString
          Oids.varcharArray, arrayCodec dString
          Oids.bpcharArray, arrayCodec dString
          Oids.jsonArray, arrayCodec dString
          Oids.jsonbArray, arrayCodec dString
          // ranges over their element's text decoder
          Oids.int4range, rangeCodec dInt4
          Oids.int8range, rangeCodec dInt8
          Oids.numrange, rangeCodec dNumeric
          Oids.daterange, rangeCodec dDate
          Oids.tsrange, rangeCodec dTimestamp
          Oids.tstzrange, rangeCodec dTimestamptz ]
        |> Map.ofList

    /// Look up a codec for `oid`: a user-registered one wins over the built-ins.
    let private lookup (oid: int) : Codec option =
        match custom.TryGetValue oid with
        | true, c -> Some c
        | _ -> Map.tryFind oid registry

    /// Whether to request `oid` in binary format.
    let hasBinary (oid: int) =
        match lookup oid with
        | Some c -> c.PreferBinary
        | None -> false

    /// Decode one column value given its type OID, wire format and raw bytes.
    /// `enc` is the connection's client_encoding (used for text-format strings).
    let decode (enc: Encoding) (oid: int) (format: int) (bytes: byte[]) : obj =
        match lookup oid with
        | Some c -> if format = 1 then c.DecodeBinary bytes else c.DecodeText(enc.GetString bytes)
        | None -> box (enc.GetString bytes) // unknown type: fall back to its text form

    // ---- escape hatch: register custom type codecs ---------------------------

    /// Build a text-format codec from a decode function (binary form = raw UTF-8).
    let textCodec (decodeText: string -> obj) : Codec =
        { PreferBinary = false
          DecodeText = decodeText
          DecodeBinary = fun b -> box (utf8.GetString b) }

    /// Build a binary-format codec from a decode function.
    let binaryCodec (decodeBinary: byte[] -> obj) : Codec =
        { PreferBinary = true
          DecodeText = box
          DecodeBinary = decodeBinary }

    /// Register (or override) the codec used to decode a given type OID. Takes
    /// effect for all connections. Returns to the built-in behavior with
    /// `unregister`.
    let register (oid: int) (codec: Codec) = custom.[oid] <- codec

    /// Convenience: register a text decoder for a type OID.
    let registerText (oid: int) (decodeText: string -> obj) = register oid (textCodec decodeText)

    let unregister (oid: int) = custom.TryRemove(oid) |> ignore

    /// True if a type OID has a registered or built-in typed codec.
    let isKnown (oid: int) = (lookup oid).IsSome

    // ---- binary parameter encoders -------------------------------------------

    let private beInt16 (v: int16) = let b = Array.zeroCreate 2 in BinaryPrimitives.WriteInt16BigEndian(Span(b), v); b
    let private beInt32 (v: int) = let b = Array.zeroCreate 4 in BinaryPrimitives.WriteInt32BigEndian(Span(b), v); b
    let private beInt64 (v: int64) = let b = Array.zeroCreate 8 in BinaryPrimitives.WriteInt64BigEndian(Span(b), v); b
    let private beUInt32 (v: uint32) = let b = Array.zeroCreate 4 in BinaryPrimitives.WriteUInt32BigEndian(Span(b), v); b
    let private beSingle (v: float32) = let b = Array.zeroCreate 4 in BinaryPrimitives.WriteSingleBigEndian(Span(b), v); b
    let private beDouble (v: float) = let b = Array.zeroCreate 8 in BinaryPrimitives.WriteDoubleBigEndian(Span(b), v); b
    let private guidBE (g: Guid) = let b = Array.zeroCreate 16 in g.TryWriteBytes(Span(b), bigEndian = true) |> ignore; b
    let private epochDay = DateOnly(2000, 1, 1).DayNumber

    let private binEncoders : Map<int, obj -> byte[]> =
        [ Oids.bool, (fun (v: obj) -> [| (if Convert.ToBoolean v then 1uy else 0uy) |])
          Oids.int2, (fun (v: obj) -> beInt16 (Convert.ToInt16 v))
          Oids.int4, (fun (v: obj) -> beInt32 (Convert.ToInt32 v))
          Oids.int8, (fun (v: obj) -> beInt64 (Convert.ToInt64 v))
          Oids.oid, (fun (v: obj) -> beUInt32 (Convert.ToUInt32 v))
          Oids.float4, (fun (v: obj) -> beSingle (Convert.ToSingle v))
          Oids.float8, (fun (v: obj) -> beDouble (Convert.ToDouble v))
          Oids.bytea, (fun (v: obj) -> v :?> byte[])
          Oids.uuid, (fun (v: obj) -> guidBE (v :?> Guid))
          Oids.date, (fun (v: obj) -> beInt32 ((v :?> DateOnly).DayNumber - epochDay))
          Oids.time, (fun (v: obj) -> beInt64 ((v :?> TimeOnly).Ticks / 10L))
          Oids.timestamp, (fun (v: obj) -> beInt64 (((v :?> DateTime) - pgEpoch).Ticks / 10L))
          Oids.timestamptz, (fun (v: obj) -> beInt64 (((v :?> DateTimeOffset).UtcDateTime - pgEpoch).Ticks / 10L)) ]
        |> Map.ofList

    // ---- parameter encoding --------------------------------------------------

    let encodeParamText (enc: Encoding) (v: obj) : byte[] =
        let s =
            match v with
            | null -> ""
            | :? string as s -> s
            | :? bool as b -> if b then "t" else "f"
            | :? int16 as x -> x.ToString(inv)
            | :? int as x -> x.ToString(inv)
            | :? int64 as x -> x.ToString(inv)
            | :? uint32 as x -> x.ToString(inv)
            | :? float32 as x -> x.ToString("R", inv)
            | :? float as x -> x.ToString("R", inv)
            | :? decimal as x -> x.ToString(inv)
            | :? Guid as g -> g.ToString("D")
            | :? (byte[]) as bytes -> "\\x" + Convert.ToHexString(bytes)
            | :? DateOnly as d -> d.ToString("yyyy-MM-dd", inv)
            | :? TimeOnly as t -> t.ToString("HH:mm:ss.FFFFFF", inv)
            | :? DateTime as dt -> dt.ToString("yyyy-MM-dd HH:mm:ss.FFFFFF", inv)
            | :? DateTimeOffset as dto -> dto.ToString("yyyy-MM-dd HH:mm:ss.FFFFFFzzz", inv)
            | other -> string other
        enc.GetBytes s

    /// Encode a parameter for a known type OID, choosing binary when an encoder
    /// exists (more compact/exact) and falling back to text otherwise. Returns
    /// (format code, bytes). Binary encoding that fails (type mismatch) also
    /// falls back to text.
    let encodeParam (enc: Encoding) (oid: int) (v: obj) : int * byte[] =
        match Map.tryFind oid binEncoders with
        | Some e ->
            try 1, e v
            with _ -> 0, encodeParamText enc v
        | None -> 0, encodeParamText enc v
