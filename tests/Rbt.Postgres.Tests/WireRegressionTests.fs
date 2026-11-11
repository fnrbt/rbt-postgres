namespace Rbt.Postgres.Tests

open System
open System.IO
open System.Text
open System.Threading
open Xunit
open Rbt.Postgres
open Rbt.Postgres.Wire

module private WireRegression =
    let message (tag: char) (body: byte[]) =
        Array.append
            [| byte tag
               byte ((body.Length + 4) >>> 24)
               byte ((body.Length + 4) >>> 16)
               byte ((body.Length + 4) >>> 8)
               byte (body.Length + 4) |]
            body

    let malformed (length: int) =
        let header =
            [| byte 'D'
               byte (length >>> 24)
               byte (length >>> 16)
               byte (length >>> 8)
               byte length |]
        use transport = new Transport(new MemoryStream(header))
        Assert.Throws<InvalidDataException>(fun () -> transport.ReadMessageAsync(CancellationToken.None).GetAwaiter().GetResult() |> ignore)
        |> ignore

[<Sealed>]
type WireRegressionTests() =
    [<Theory>]
    [<InlineData(0)>]
    [<InlineData(3)>]
    [<InlineData(-1)>]
    [<InlineData(1073741824)>]
    [<InlineData(2147483647)>]
    member _.``invalid frame lengths fail before reading or allocating a body``(length: int) =
        WireRegression.malformed length

    [<Fact>]
    member _.``header-only maximal valid frame fails with EOF without allocating its claimed body``() =
        let length = MaxMessageLength
        let header =
            [| byte 'D'
               byte (length >>> 24)
               byte (length >>> 16)
               byte (length >>> 8)
               byte length |]
        use transport = new Transport(new MemoryStream(header))
        Assert.Throws<EndOfStreamException>(fun () -> transport.ReadMessageAsync(CancellationToken.None).GetAwaiter().GetResult() |> ignore)
        |> ignore

    [<Fact>]
    member _.``empty frames consume their header and allow the next frame``() =
        let bytes = Array.append (WireRegression.message '1' [||]) (WireRegression.message '2' [| 42uy |])
        use transport = new Transport(new MemoryStream(bytes))
        let first = transport.ReadMessageAsync(CancellationToken.None).GetAwaiter().GetResult()
        let second = transport.ReadMessageAsync(CancellationToken.None).GetAwaiter().GetResult()
        Assert.Equal('1', first.Tag)
        Assert.Equal(0, first.Remaining)
        Assert.Equal('2', second.Tag)
        Assert.Equal(42uy, second.Byte())

    [<Fact>]
    member _.``cancellation is honored when the entire next frame is already buffered``() =
        let bytes = Array.append (WireRegression.message '1' [||]) (WireRegression.message '2' [| 42uy |])
        use transport = new Transport(new MemoryStream(bytes))
        use cts = new CancellationTokenSource()
        transport.ReadMessageAsync(cts.Token).GetAwaiter().GetResult() |> ignore
        Assert.True(transport.HasBufferedRead)
        cts.Cancel()
        Assert.ThrowsAny<OperationCanceledException>(fun () -> transport.ReadMessageAsync(cts.Token).GetAwaiter().GetResult() |> ignore)
        |> ignore
        Assert.True(transport.HasBufferedRead)
        Assert.ThrowsAny<OperationCanceledException>(fun () -> transport.ReadRawByteAsync(cts.Token).GetAwaiter().GetResult() |> ignore)
        |> ignore

    [<Fact>]
    member _.``transport decoding follows the configured client encoding``() =
        let notification = WireRegression.message 'A' [| 0uy; 0uy; 0uy; 1uy; 0xC3uy; 0xA9uy; 0uy; 0xC3uy; 0xA9uy; 0uy |]
        let error = WireRegression.message 'E' [| byte 'M'; 0xE9uy; 0uy; 0uy |]
        use transport = new Transport(new MemoryStream(Array.append notification error))
        Assert.Equal(Encoding.UTF8.WebName, transport.Encoding.WebName)
        let m = transport.ReadMessageAsync(CancellationToken.None).GetAwaiter().GetResult()
        let pid, channel, payload = Messages.parseNotification m
        Assert.Equal(1, pid)
        Assert.Equal("é", channel)
        Assert.Equal("é", payload)
        Assert.True(transport.HasBufferedRead)
        transport.Encoding <- Encoding.Latin1
        let fields = transport.ReadMessageAsync(CancellationToken.None).GetAwaiter().GetResult() |> ErrorFields.parse
        Assert.Equal("é", fields.MessageText)
        Assert.Equal("é", IncomingMessage('X', [| 0xE9uy; 0uy |], Encoding.Latin1).CString())
        Assert.Equal("é", IncomingMessage('X', [| 0xC3uy; 0xA9uy; 0uy |]).CString())

    [<Fact>]
    member _.``text bind encodes portal statement and parameter with client encoding``() =
        let bytes = Messages.bind Encoding.Latin1 "pé" "sté" [ Some "é"; None ] |> fun m -> m.ToBytes()
        Assert.Equal(byte 'B', bytes.[0])
        let m = IncomingMessage('B', bytes.[5..], Encoding.Latin1)
        Assert.Equal("pé", m.CString())
        Assert.Equal("sté", m.CString())
        Assert.Equal(0, m.UInt16())
        Assert.Equal(2, m.UInt16())
        Assert.Equal(1, m.Int32())
        Assert.Equal(0xE9uy, m.Byte())
        Assert.Equal(-1, m.Int32())
        Assert.Equal(0, m.UInt16())
        Assert.True(m.AtEnd)
        Assert.Contains(0xE9uy, bytes)

    [<Fact>]
    member _.``unsigned backend counts retain values above signed short range``() =
        let count = 32768
        let parameterBody = Array.zeroCreate<byte> (2 + count * 4)
        parameterBody.[0] <- 0x80uy
        let parameters = Messages.parseParameterDescription(IncomingMessage('t', parameterBody))
        Assert.Equal(count, parameters.Length)
        let rowBody = Array.zeroCreate<byte> (2 + count * 19)
        rowBody.[0] <- 0x80uy
        let last = 2 + (count - 1) * 19
        rowBody.[last + 5] <- 0xFFuy
        rowBody.[last + 6] <- 0xFFuy
        rowBody.[last + 11] <- 0xFFuy
        rowBody.[last + 12] <- 0xFFuy
        rowBody.[last + 17] <- 0xFFuy
        rowBody.[last + 18] <- 0xFFuy
        let fields = Messages.parseRowDescription(IncomingMessage('T', rowBody))
        Assert.Equal(count, fields.Length)
        Assert.Equal(-1, fields.[count - 1].ColumnAttr)
        Assert.Equal(-1, fields.[count - 1].TypeSize)
        Assert.Equal(-1, fields.[count - 1].FormatCode)
        let dataBody = Array.zeroCreate<byte> (2 + count * 4)
        dataBody.[0] <- 0x80uy
        for i in 0 .. count - 1 do
            dataBody.[2 + i * 4] <- 0xFFuy
            dataBody.[3 + i * 4] <- 0xFFuy
            dataBody.[4 + i * 4] <- 0xFFuy
            dataBody.[5 + i * 4] <- 0xFFuy
        let columns = Messages.parseDataRow(IncomingMessage('D', dataBody))
        Assert.Equal(count, columns.Length)
        Assert.Equal(None, columns.[count - 1])
