namespace Rbt.Postgres

open System
open System.Security.Cryptography
open Rbt.Postgres.Wire

/// SCRAM-SHA-256 and SCRAM-SHA-256-PLUS (RFC 5802 / RFC 7677). SCRAM is the
/// default authentication mechanism for modern PostgreSQL; the -PLUS variant
/// adds `tls-server-end-point` channel binding over a TLS connection.
module Scram =

    /// Which gs2 channel-binding flag the client advertises.
    type ChannelBinding =
        | CbNone // "n,,"  — no TLS, no binding
        | CbNotUsed // "y,,"  — TLS up, client supports binding but server didn't offer -PLUS
        | CbTlsServerEndPoint of byte[] // "p=tls-server-end-point,," + cert hash

    let private gs2Header =
        function
        | CbNone -> "n,,"
        | CbNotUsed -> "y,,"
        | CbTlsServerEndPoint _ -> "p=tls-server-end-point,,"

    /// The input to the base64-encoded `c=` field: the gs2 header bytes followed
    /// by the channel-binding data (only present for -PLUS).
    let private cbindInput cb =
        let header = utf8.GetBytes(gs2Header cb)
        match cb with
        | CbTlsServerEndPoint data -> Array.append header data
        | _ -> header

    let private hmacSha256 (key: byte[]) (data: byte[]) =
        use h = new HMACSHA256(key)
        h.ComputeHash(data)

    let private sha256 (data: byte[]) =
        use h = SHA256.Create()
        h.ComputeHash(data)

    let private xor (a: byte[]) (b: byte[]) = Array.map2 (^^^) a b

    type ScramState =
        { Password: string
          ClientNonce: string
          ClientFirstBare: string
          Binding: ChannelBinding }

    /// Build client-first-message and the SASL initial response payload.
    let start (password: string) (cb: ChannelBinding) : ScramState * byte[] =
        let nonceBytes = RandomNumberGenerator.GetBytes(18)
        let clientNonce = Convert.ToBase64String(nonceBytes)
        let clientFirstBare = $"n=,r={clientNonce}"
        let clientFirst = (gs2Header cb) + clientFirstBare
        let state =
            { Password = password
              ClientNonce = clientNonce
              ClientFirstBare = clientFirstBare
              Binding = cb }
        state, utf8.GetBytes clientFirst

    let private parseAttrs (s: string) =
        s.Split(',')
        |> Array.choose (fun part ->
            let eq = part.IndexOf('=')
            if eq > 0 then Some(part.[0], part.Substring(eq + 1)) else None)
        |> Map.ofArray

    // Generated with Python's Unicode 3.2 `stringprep` module over code
    // points 0..0x10FFFF, in this order: in_table_a1, in_table_b1,
    // in_table_c12, union of in_table_c12/c21/c22/c3/c4/c5/c6/c7/c8/c9,
    // in_table_d1, in_table_d2. Each table consists of consecutive inclusive
    // ranges; encode its count followed by endpoints as little-endian Int32,
    // concatenate the tables, gzip, then base64 encode.
    let private stringprepTables =
        let data = Convert.FromBase64String("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")
        use input = new System.IO.MemoryStream(data)
        use gzip = new System.IO.Compression.GZipStream(input, System.IO.Compression.CompressionMode.Decompress)
        use reader = new System.IO.BinaryReader(gzip)
        Array.init 6 (fun _ ->
            let count = reader.ReadInt32()
            Array.init count (fun _ -> reader.ReadInt32()))

    let private inTable table value =
        let ranges: int[] = stringprepTables.[table]
        let mutable lo = 0
        let mutable hi = ranges.Length / 2 - 1
        let mutable found = false
        while lo <= hi && not found do
            let mid = lo + (hi - lo) / 2
            if value < ranges.[2 * mid] then hi <- mid - 1
            elif value > ranges.[2 * mid + 1] then lo <- mid + 1
            else found <- true
        found

    // PostgreSQL uses the original password if SASLprep fails. Rejecting
    // Unicode 3.2 unassigned input before normalization also prevents newer
    // Unicode versions from assigning it a compatibility decomposition.
    let private saslprep (password: string) =
        try
            let mapped = System.Text.StringBuilder(password.Length)
            let mutable invalid = false
            let mutable i = 0
            while i < password.Length do
                let c = password.[i]
                let cp, width =
                    if Char.IsHighSurrogate(c) && i + 1 < password.Length && Char.IsLowSurrogate(password.[i + 1]) then
                        Char.ConvertToUtf32(c, password.[i + 1]), 2
                    elif Char.IsSurrogate(c) then
                        invalid <- true
                        int c, 1
                    else int c, 1
                if inTable 0 cp then invalid <- true
                elif inTable 1 cp then ()
                elif inTable 2 cp then mapped.Append(' ') |> ignore
                else
                    // Five normalization corrections made after Unicode 3.2.
                    let old =
                        match cp with
                        | 0x2f868 -> 0x2136a | 0x2f874 -> 0x5f33
                        | 0x2f91f -> 0x43ab | 0x2f95f -> 0x7aae
                        | 0x2f9bf -> 0x4d57 | _ -> cp
                    mapped.Append(Char.ConvertFromUtf32(old)) |> ignore
                i <- i + width
            if invalid then password
            else
                let normalized = mapped.ToString().Normalize(System.Text.NormalizationForm.FormKC)
                let mutable hasRandAL = false
                let mutable hasL = false
                let mutable first = -1
                let mutable last = -1
                let mutable j = 0
                while j < normalized.Length do
                    let cp = Char.ConvertToUtf32(normalized, j)
                    if first = -1 then first <- cp
                    last <- cp
                    if inTable 0 cp || inTable 3 cp then invalid <- true
                    if inTable 4 cp then hasRandAL <- true
                    if inTable 5 cp then hasL <- true
                    j <- j + (if cp > 0xffff then 2 else 1)
                if hasRandAL && (hasL || not (inTable 4 first) || not (inTable 4 last)) then invalid <- true
                if invalid then password else normalized
        with :? ArgumentException -> password

    /// Given server-first-message, produce the client-final-message payload and
    /// the expected server signature (base64) for later verification.
    let finish (state: ScramState) (serverFirst: byte[]) : byte[] * string =
        let serverFirstStr = utf8.GetString serverFirst
        let attrs = parseAttrs serverFirstStr
        let serverNonce = attrs.['r']
        let salt = Convert.FromBase64String attrs.['s']
        let iterations = int attrs.['i']
        // A malicious server must not be able to demand unbounded CPU work.
        if iterations < 1 || iterations > 1000000 then
            failwith "SCRAM: iteration count outside supported range."

        if not (serverNonce.StartsWith state.ClientNonce) then
            failwith "SCRAM: server nonce does not extend client nonce (possible MITM)."

        let saltedPassword =
            Rfc2898DeriveBytes.Pbkdf2(utf8.GetBytes(saslprep state.Password), salt, iterations, HashAlgorithmName.SHA256, 32)

        let clientKey = hmacSha256 saltedPassword (utf8.GetBytes "Client Key")
        let storedKey = sha256 clientKey
        let serverKey = hmacSha256 saltedPassword (utf8.GetBytes "Server Key")

        let channelBinding = $"c={Convert.ToBase64String(cbindInput state.Binding)},r={serverNonce}"
        let authMessage = state.ClientFirstBare + "," + serverFirstStr + "," + channelBinding
        let authBytes = utf8.GetBytes authMessage

        let clientSignature = hmacSha256 storedKey authBytes
        let clientProof = xor clientKey clientSignature
        let serverSignature = hmacSha256 serverKey authBytes

        let clientFinal = $"{channelBinding},p={Convert.ToBase64String clientProof}"
        utf8.GetBytes clientFinal, Convert.ToBase64String serverSignature

    /// Verify the server's SASLFinal "v=..." signature.
    let verifyServer (expectedServerSignature: string) (saslFinal: byte[]) =
        let attrs = parseAttrs (utf8.GetString saslFinal)
        match Map.tryFind 'v' attrs with
        | Some v when v = expectedServerSignature -> ()
        | Some v -> failwith $"SCRAM: server signature mismatch (got {v})."
        | None -> failwith "SCRAM: server final message missing signature."
