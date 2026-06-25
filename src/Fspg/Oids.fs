namespace Fspg

/// Object identifiers for the built-in PostgreSQL types this client decodes.
/// (From the catalog; stable across versions.)
module Oids =
    let bool = 16
    let bytea = 17
    let char = 18 // "char" (1 byte)
    let name = 19
    let int8 = 20
    let int2 = 21
    let int4 = 23
    let text = 25
    let oid = 26
    let json = 114
    let float4 = 700
    let float8 = 701
    let bpchar = 1042
    let varchar = 1043
    let date = 1082
    let time = 1083
    let timestamp = 1114
    let timestamptz = 1184
    let interval = 1186
    let numeric = 1700
    let uuid = 2950
    let jsonb = 3802

    // Array OIDs (element-type OID is mapped in Codecs).
    let boolArray = 1000
    let byteaArray = 1001
    let charArray = 1002
    let nameArray = 1003
    let int2Array = 1005
    let int4Array = 1007
    let textArray = 1009
    let bpcharArray = 1014
    let varcharArray = 1015
    let int8Array = 1016
    let float4Array = 1021
    let float8Array = 1022
    let oidArray = 1028
    let timestampArray = 1115
    let dateArray = 1182
    let timeArray = 1183
    let timestamptzArray = 1185
    let numericArray = 1231
    let jsonArray = 199
    let jsonbArray = 3807
    let uuidArray = 2951

    // Range types
    let int4range = 3904
    let numrange = 3906
    let tsrange = 3908
    let tstzrange = 3910
    let daterange = 3912
    let int8range = 3926
