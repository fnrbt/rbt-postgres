namespace Fspg

open System
open System.Buffers
open System.IO
open System.Text
open System.Threading
open System.Threading.Tasks

/// Low-level reading/writing of the PostgreSQL frontend/backend protocol v3.0.
/// All multi-byte integers on the wire are big-endian (network byte order).
module Wire =

    let utf8 = UTF8Encoding(false)

    // ---- Big-endian primitives ----------------------------------------------

    let writeInt32BE (buf: MemoryStream) (value: int) =
        buf.WriteByte(byte (value >>> 24))
        buf.WriteByte(byte (value >>> 16))
        buf.WriteByte(byte (value >>> 8))
        buf.WriteByte(byte value)

    let writeInt16BE (buf: MemoryStream) (value: int) =
        buf.WriteByte(byte (value >>> 8))
        buf.WriteByte(byte value)

    let writeCString (buf: MemoryStream) (value: string) =
        let bytes = utf8.GetBytes(value)
        buf.Write(bytes, 0, bytes.Length)
        buf.WriteByte(0uy)

    // ---- Frontend message builder -------------------------------------------

    /// Builds a single frontend message: an optional 1-byte type tag followed by
    /// an Int32 length (covering the length field itself and the body).
    /// `encoding` is used for CString text (defaults to UTF-8); pass the
    /// connection's client_encoding for messages carrying user SQL.
    type OutgoingMessage(?tag: char, ?encoding: Encoding) =
        let body = new MemoryStream()
        let enc = defaultArg encoding utf8
        member _.Int32(v: int) = writeInt32BE body v
        member _.Int16(v: int) = writeInt16BE body v

        member _.Int64(v: int64) =
            for shift in [ 56; 48; 40; 32; 24; 16; 8; 0 ] do
                body.WriteByte(byte (v >>> shift))

        member _.Byte(v: byte) = body.WriteByte v

        member _.CString(v: string) =
            let bytes = enc.GetBytes v
            body.Write(bytes, 0, bytes.Length)
            body.WriteByte 0uy

        member _.Bytes(v: byte[]) = body.Write(v, 0, v.Length)

        /// Serialize the framed message to a byte array.
        member _.ToBytes() =
            let payload = body.ToArray()
            let length = payload.Length + 4 // length field includes itself
            use framed = new MemoryStream()
            match tag with
            | Some t -> framed.WriteByte(byte t)
            | None -> ()
            writeInt32BE framed length
            framed.Write(payload, 0, payload.Length)
            framed.ToArray()

    // ---- Backend message reader (cursor over one message body) --------------

    type IncomingMessage(tag: char, body: byte[]) =
        let mutable pos = 0
        member _.Tag = tag
        member _.Body = body
        member _.Remaining = body.Length - pos
        member _.AtEnd = pos >= body.Length

        member _.Int32() =
            let v =
                (int body.[pos] <<< 24)
                ||| (int body.[pos + 1] <<< 16)
                ||| (int body.[pos + 2] <<< 8)
                ||| (int body.[pos + 3])
            pos <- pos + 4
            v

        member _.Int16() =
            let v = int (int16 ((int body.[pos] <<< 8) ||| int body.[pos + 1]))
            pos <- pos + 2
            v

        member _.Int64() =
            let mutable v = 0L
            for k in 0..7 do
                v <- (v <<< 8) ||| int64 body.[pos + k]
            pos <- pos + 8
            v

        member _.Byte() =
            let v = body.[pos]
            pos <- pos + 1
            v

        member _.CString() =
            let start = pos
            while body.[pos] <> 0uy do
                pos <- pos + 1
            let s = utf8.GetString(body, start, pos - start)
            pos <- pos + 1
            s

        member _.Bytes(n: int) =
            let slice = Array.sub body pos n
            pos <- pos + n
            slice

        member _.RestBytes() =
            let slice = Array.sub body pos (body.Length - pos)
            pos <- body.Length
            slice

    // ---- Async buffered transport -------------------------------------------

    /// Buffered, asynchronous framing over a duplex stream (NetworkStream or
    /// SslStream). Reads reuse a pooled scratch buffer; outgoing messages are
    /// batched into a single write+flush to cut round-trips and syscalls.
    type Transport(inner: Stream) =
        let mutable rbuf : byte[] = ArrayPool<byte>.Shared.Rent(16384)
        let mutable rpos = 0 // start of unconsumed data
        let mutable rend = 0 // end of valid data
        let wbuf = new MemoryStream()
        let mutable disposed = false

        member _.Inner = inner

        /// Ensure at least `need` contiguous bytes are buffered at rpos.
        member private _.EnsureAsync(need: int, ct: CancellationToken) : Task =
            task {
                if rend - rpos < need then
                    // Compact unconsumed bytes to the front of the buffer.
                    if rpos > 0 then
                        Buffer.BlockCopy(rbuf, rpos, rbuf, 0, rend - rpos)
                        rend <- rend - rpos
                        rpos <- 0
                    // Grow if the message is larger than the current buffer.
                    if rbuf.Length < need then
                        let bigger = ArrayPool<byte>.Shared.Rent(max need (rbuf.Length * 2))
                        Buffer.BlockCopy(rbuf, 0, bigger, 0, rend)
                        ArrayPool<byte>.Shared.Return(rbuf)
                        rbuf <- bigger
                    while rend - rpos < need do
                        let! n = inner.ReadAsync(rbuf.AsMemory(rend, rbuf.Length - rend), ct)
                        if n <= 0 then
                            raise (EndOfStreamException("Connection closed by server while reading."))
                        rend <- rend + n
            }

        /// Read one framed backend message: 1-byte tag, Int32 length, body.
        member this.ReadMessageAsync(ct: CancellationToken) : Task<IncomingMessage> =
            task {
                do! this.EnsureAsync(5, ct)
                let tag = char rbuf.[rpos]
                let length =
                    (int rbuf.[rpos + 1] <<< 24)
                    ||| (int rbuf.[rpos + 2] <<< 16)
                    ||| (int rbuf.[rpos + 3] <<< 8)
                    ||| (int rbuf.[rpos + 4])
                let bodyLen = length - 4
                do! this.EnsureAsync(5 + bodyLen, ct)
                let body = if bodyLen > 0 then Array.sub rbuf (rpos + 5) bodyLen else [||]
                rpos <- rpos + 5 + bodyLen
                return IncomingMessage(tag, body)
            }

        /// Read a single raw byte (the SSLRequest 'S'/'N' reply is unframed).
        member this.ReadRawByteAsync(ct: CancellationToken) : Task<byte> =
            task {
                do! this.EnsureAsync(1, ct)
                let b = rbuf.[rpos]
                rpos <- rpos + 1
                return b
            }

        /// Queue a frontend message into the write buffer (not yet sent).
        member _.Enqueue(msg: OutgoingMessage) =
            let bytes = msg.ToBytes()
            wbuf.Write(bytes, 0, bytes.Length)

        /// Write raw bytes directly (used for the unframed SSLRequest).
        member _.EnqueueRaw(bytes: byte[]) = wbuf.Write(bytes, 0, bytes.Length)

        /// Flush all queued messages as one write, then flush the stream.
        member _.FlushAsync(ct: CancellationToken) : Task =
            task {
                if wbuf.Length > 0L then
                    let len = int wbuf.Length
                    do! inner.WriteAsync(wbuf.GetBuffer().AsMemory(0, len), ct)
                    do! inner.FlushAsync(ct)
                    wbuf.SetLength(0L)
            }

        /// Queue one message and flush immediately.
        member this.SendAsync(msg: OutgoingMessage, ct: CancellationToken) : Task =
            task {
                this.Enqueue(msg)
                do! this.FlushAsync(ct)
            }

        member _.HasBufferedRead = rend - rpos > 0

        member _.Dispose() =
            if not disposed then
                disposed <- true
                ArrayPool<byte>.Shared.Return(rbuf)
                wbuf.Dispose()
                inner.Dispose()

        interface IDisposable with
            member this.Dispose() = this.Dispose()
