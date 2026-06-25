namespace Fspg

open System
open System.Security.Cryptography
open Fspg.Wire

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

    /// Given server-first-message, produce the client-final-message payload and
    /// the expected server signature (base64) for later verification.
    let finish (state: ScramState) (serverFirst: byte[]) : byte[] * string =
        let serverFirstStr = utf8.GetString serverFirst
        let attrs = parseAttrs serverFirstStr
        let serverNonce = attrs.['r']
        let salt = Convert.FromBase64String attrs.['s']
        let iterations = int attrs.['i']

        if not (serverNonce.StartsWith state.ClientNonce) then
            failwith "SCRAM: server nonce does not extend client nonce (possible MITM)."

        let saltedPassword =
            Rfc2898DeriveBytes.Pbkdf2(utf8.GetBytes state.Password, salt, iterations, HashAlgorithmName.SHA256, 32)

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
