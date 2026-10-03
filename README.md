# xmip-core-transport-redis-streams

Redis Streams transport: one entry is one Stream, the key and id beside it; a Location appends with XADD or reads on from its cursor, or accepts clients directly. RESP2. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

A Send Location appends with XADD on a connection kept per server (`transport::Pool`). Until 2026-09-27 every send connected.

A Receive Location reads with XRANGE on the same kept connection, after the transport's cursor. Until 2026-09-28 every receive connected.

An entry is acknowledged after the runtime's whole receive cycle, not as it is read; XRANGE consumes nothing on the server. `Accepted` moves the cursor to the entry, and only from the entry it was read after, so the cursor advances contiguously (`transport::contiguous::Contiguous`); `Refused` moves it the same way, since a log has no place to reject an entry into and a refused entry is not read again; `Failed` leaves it, and the failed entry and those after it are read again — at least once, never a skip. No consumer group is kept, so there is nothing to XACK: the acknowledgement is an in-memory step with no round trip. Until 2026-10-02 a read moved the cursor past what it read.

A send target is read by `net::Target` in [xmip-core-library-net](https://github.com/IlleNilsson/xmip-core-library-net), the one reading of a URI every technology calls: scheme, authority, path and decoded query. Until 2026-09-28 it was read through the transport capability's `socket::target`, which split it on its first slash and left the query in the path.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
