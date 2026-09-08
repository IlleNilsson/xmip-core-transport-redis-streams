# xmip-core-transport-redis-streams

Redis Streams transport: one entry is one Stream, the key and id beside it; a Location appends with XADD or reads on from its cursor, or accepts clients directly. RESP2. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
