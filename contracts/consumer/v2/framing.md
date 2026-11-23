# ConsumerGateway Wire Contract V2 framing

Every integer is unsigned and encoded in network byte order (big endian). The
fixed header is exactly 32 bytes:

| Offset | Width | Field | Required value or range |
|---:|---:|---|---|
| 0 | 4 | magic | ASCII `XTC2` |
| 4 | 1 | header_version | `1` |
| 5 | 1 | message_kind | `1=request`, `2=response`, `3=event`, `4=error` |
| 6 | 2 | flags | `0`; unknown bits are rejected |
| 8 | 2 | api_major | `2` |
| 10 | 2 | api_minor | `0` |
| 12 | 2 | payload_schema_version | `2` |
| 14 | 2 | reserved | `0` |
| 16 | 8 | correlation_id | `0..2^64-1` |
| 24 | 4 | payload_length | `0..1048576` |
| 28 | 4 | payload_crc32 | IEEE CRC-32 of payload bytes |

The payload is canonical UTF-8 JSON (`sort_keys`, no insignificant whitespace).
A receiver must fully collect the declared frame, reject extra bytes, validate
CRC, decode UTF-8/JSON, and apply the matching Draft 2020-12 schema in that
order. It must never advance an event cursor for a rejected frame.
