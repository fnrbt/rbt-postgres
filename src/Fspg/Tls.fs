namespace Fspg

open System
open System.IO
open System.Net.Security
open System.Security.Authentication
open System.Security.Cryptography
open System.Security.Cryptography.X509Certificates
open System.Threading
open System.Threading.Tasks

/// libpq-compatible TLS modes.
type SslMode =
    | Disable // never use TLS
    | Allow // plaintext, fall back to TLS only if the server rejects plaintext
    | Prefer // try TLS, fall back to plaintext if the server has no SSL
    | Require // must use TLS, but don't verify the certificate
    | VerifyCa // must use TLS and verify the cert chains to a trusted CA
    | VerifyFull // verify-ca plus the server hostname matches the certificate

type SslConfig =
    { Mode: SslMode
      /// PEM file with the trusted CA root(s) for verify-ca / verify-full.
      RootCertPath: string option
      /// Optional client certificate for mutual TLS.
      ClientCertificate: X509Certificate2 option
      /// Hostname validated in verify-full (and used for SNI).
      TargetHost: string }

module SslConfig =
    let defaultFor (host: string) =
        { Mode = Prefer
          RootCertPath = None
          ClientCertificate = None
          TargetHost = host }

/// Outcome of TLS negotiation: the stream the protocol should use from here on,
/// whether it is encrypted, and the server certificate (for channel binding).
type TlsResult =
    { Stream: Stream
      Encrypted: bool
      ServerCertificate: X509Certificate2 option }

module Tls =

    // SSLRequest packet: Int32 length=8, Int32 code=80877103 (0x04d2162f).
    let private sslRequest = [| 0uy; 0uy; 0uy; 8uy; 0x04uy; 0xd2uy; 0x16uy; 0x2fuy |]

    let private asCert2 (cert: X509Certificate) =
        match cert with
        | :? X509Certificate2 as c -> c
        | _ -> new X509Certificate2(cert)

    /// The `tls-server-end-point` channel-binding value (RFC 5929): a hash of
    /// the server's DER certificate using the cert's own signature hash, with
    /// MD5/SHA-1 promoted to SHA-256.
    let channelBindingHash (cert: X509Certificate2) : byte[] =
        let hash : HashAlgorithm =
            match cert.SignatureAlgorithm.Value with
            | "1.2.840.113549.1.1.12" | "1.2.840.10045.4.3.3" -> SHA384.Create() // sha384 RSA/ECDSA
            | "1.2.840.113549.1.1.13" | "1.2.840.10045.4.3.4" -> SHA512.Create() // sha512 RSA/ECDSA
            | _ -> SHA256.Create() // sha256 and the MD5/SHA-1 promotion case
        use hash = hash
        hash.ComputeHash(cert.RawData)

    /// Build the certificate chain, optionally pinning a custom CA root.
    let private chainTrusted (cfg: SslConfig) (cert: X509Certificate) =
        use chain = new X509Chain()
        chain.ChainPolicy.RevocationMode <- X509RevocationMode.NoCheck
        match cfg.RootCertPath with
        | Some path ->
            let root = X509CertificateLoader.LoadCertificateFromFile(path)
            chain.ChainPolicy.TrustMode <- X509ChainTrustMode.CustomRootTrust
            chain.ChainPolicy.CustomTrustStore.Add(root) |> ignore
        | None -> ()
        chain.Build(asCert2 cert)

    let private makeValidator (cfg: SslConfig) =
        RemoteCertificateValidationCallback(fun _ cert _ errors ->
            match cfg.Mode with
            // require/prefer/allow: encrypt the channel but do not verify the cert
            | Disable | Allow | Prefer | Require -> true
            | VerifyCa -> chainTrusted cfg cert
            | VerifyFull ->
                chainTrusted cfg cert
                && (int (errors &&& SslPolicyErrors.RemoteCertificateNameMismatch) = 0))

    let private handshakeAsync (netStream: Stream) (cfg: SslConfig) (ct: CancellationToken) : Task<TlsResult> =
        task {
            let ssl = new SslStream(netStream, false, makeValidator cfg)
            let opts = SslClientAuthenticationOptions(TargetHost = cfg.TargetHost, EnabledSslProtocols = SslProtocols.None)
            match cfg.ClientCertificate with
            | Some c -> opts.ClientCertificates <- X509CertificateCollection([| c :> X509Certificate |])
            | None -> ()
            do! ssl.AuthenticateAsClientAsync(opts, ct)
            let serverCert = ssl.RemoteCertificate |> Option.ofObj |> Option.map asCert2
            return { Stream = ssl; Encrypted = true; ServerCertificate = serverCert }
        }

    /// Perform the SSLRequest exchange on the raw stream and, if the server
    /// agrees, upgrade to TLS. Returns the stream the caller should use next.
    let negotiateAsync (netStream: Stream) (cfg: SslConfig) (ct: CancellationToken) : Task<TlsResult> =
        task {
            match cfg.Mode with
            | Disable ->
                return { Stream = netStream; Encrypted = false; ServerCertificate = None }
            | _ ->
                do! netStream.WriteAsync(System.ReadOnlyMemory(sslRequest), ct)
                do! netStream.FlushAsync(ct)
                let reply = Array.zeroCreate<byte> 1
                let! n = netStream.ReadAsync(reply.AsMemory(0, 1), ct)
                if n <> 1 then
                    return raise (EndOfStreamException("Server closed connection during SSL negotiation."))
                else
                    match char reply.[0] with
                    | 'S' -> return! handshakeAsync netStream cfg ct
                    | 'N' ->
                        match cfg.Mode with
                        | Require
                        | VerifyCa
                        | VerifyFull ->
                            return failwith "Server does not support SSL, but sslmode requires it."
                        | _ -> // Prefer / Allow: continue in plaintext
                            return { Stream = netStream; Encrypted = false; ServerCertificate = None }
                    | other -> return failwith $"Unexpected SSL negotiation reply byte '{other}'."
        }
