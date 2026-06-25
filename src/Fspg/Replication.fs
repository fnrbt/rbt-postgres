namespace Fspg

open Fspg.Wire

/// Decoder for the `pgoutput` logical replication message format (protocol
/// version 1) — what `CREATE SUBSCRIPTION` / real CDC streams. Each XLogData
/// payload from a pgoutput slot is one of these messages.
module PgOutput =

    /// A decoded tuple: per-column text value, None for NULL / unchanged-TOAST.
    type Tuple = string option []

    type Message =
        | Begin of finalLsn: int64 * xid: int
        | Commit of commitLsn: int64 * endLsn: int64
        | Relation of oid: int * schema: string * name: string * columns: string[]
        | Insert of relationOid: int * newTuple: Tuple
        | Update of relationOid: int * newTuple: Tuple
        | Delete of relationOid: int * oldTuple: Tuple
        | Truncate of relationOids: int[]
        | Origin of name: string
        | TypeInfo of oid: int * name: string
        | Other of tag: char

    let private parseTuple (m: IncomingMessage) : Tuple =
        let n = m.Int16()
        Array.init n (fun _ ->
            match char (m.Byte()) with
            | 'n' | 'u' -> None // null / unchanged TOAST
            | 't' | 'b' ->
                let len = m.Int32()
                Some(utf8.GetString(m.Bytes len))
            | _ -> None)

    /// Parse one pgoutput message from an XLogData payload.
    let parse (payload: byte[]) : Message =
        let tag = char payload.[0]
        let m = IncomingMessage(tag, payload.[1..])
        match tag with
        | 'B' ->
            let lsn = m.Int64()
            m.Int64() |> ignore // commit timestamp
            Begin(lsn, m.Int32())
        | 'C' ->
            m.Byte() |> ignore // flags
            let clsn = m.Int64()
            let elsn = m.Int64()
            m.Int64() |> ignore // commit timestamp
            Commit(clsn, elsn)
        | 'R' ->
            let oid = m.Int32()
            let schema = m.CString()
            let name = m.CString()
            m.Byte() |> ignore // replica identity
            let ncol = m.Int16()
            let cols =
                Array.init ncol (fun _ ->
                    m.Byte() |> ignore // flags
                    let cname = m.CString()
                    m.Int32() |> ignore // type oid
                    m.Int32() |> ignore // type modifier
                    cname)
            Relation(oid, schema, name, cols)
        | 'I' ->
            let relOid = m.Int32()
            m.Byte() |> ignore // 'N'
            Insert(relOid, parseTuple m)
        | 'U' ->
            let relOid = m.Int32()
            // optional old tuple ('K' key / 'O' old), then 'N' new tuple
            let mutable marker = char (m.Byte())
            if marker = 'K' || marker = 'O' then
                parseTuple m |> ignore
                marker <- char (m.Byte())
            Update(relOid, parseTuple m)
        | 'D' ->
            let relOid = m.Int32()
            m.Byte() |> ignore // 'K' or 'O'
            Delete(relOid, parseTuple m)
        | 'T' ->
            let n = m.Int32()
            m.Byte() |> ignore // flags
            Truncate(Array.init n (fun _ -> m.Int32()))
        | 'O' ->
            m.Int64() |> ignore
            Origin(m.CString())
        | 'Y' ->
            let oid = m.Int32()
            m.CString() |> ignore // schema
            TypeInfo(oid, m.CString())
        | other -> Other other

/// Helpers for the streaming replication protocol, layered on a Connection that
/// was opened with ReplicationMode set. IDENTIFY_SYSTEM and CREATE/DROP slot
/// commands return ordinary result sets; START_REPLICATION enters COPY-BOTH and
/// is driven by Connection.StreamReplicationAsync.
module Replication =

    type SystemIdentity =
        { SystemId: string
          Timeline: int
          XLogPos: string
          DbName: string option }

    let private cell (rs: ResultSet) (name: string) =
        match rs.Columns |> Array.tryFindIndex (fun c -> c.Name = name) with
        | Some i -> (List.head rs.Rows).[i]
        | None -> None

    /// Run IDENTIFY_SYSTEM and parse the single-row result.
    let identifySystem (conn: Connection) : SystemIdentity =
        let rs = conn.Query("IDENTIFY_SYSTEM") |> List.head
        { SystemId = cell rs "systemid" |> Option.defaultValue ""
          Timeline = cell rs "timeline" |> Option.map int |> Option.defaultValue 0
          XLogPos = cell rs "xlogpos" |> Option.defaultValue ""
          DbName = cell rs "dbname" }

    /// Create a logical replication slot using the given output plugin
    /// (e.g. "test_decoding" or "pgoutput").
    let createLogicalSlot (conn: Connection) (slot: string) (plugin: string) =
        conn.Query(sprintf "CREATE_REPLICATION_SLOT %s LOGICAL %s" slot plugin) |> ignore

    /// Create a physical replication slot.
    let createPhysicalSlot (conn: Connection) (slot: string) =
        conn.Query(sprintf "CREATE_REPLICATION_SLOT %s PHYSICAL" slot) |> ignore

    let dropSlot (conn: Connection) (slot: string) =
        try
            conn.Query(sprintf "DROP_REPLICATION_SLOT %s" slot) |> ignore
        with _ -> ()

    /// Stream a logical slot from the given LSN ("0/0" = slot's restart point),
    /// invoking `onChange` with each decoded payload; return false to stop.
    let streamLogical
        (conn: Connection)
        (slot: string)
        (startLsn: string)
        (onChange: int64 * byte[] -> bool)
        : System.Threading.Tasks.Task =
        conn.StreamReplicationAsync(sprintf "START_REPLICATION SLOT %s LOGICAL %s" slot startLsn, onChange)
