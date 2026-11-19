# Xenophyte P2P client receive optimization

## Changes

- Read XENO frame headers before allocating payload buffers.
- For fragmented logical packets, read each fragment header and metadata into fixed-size scratch buffers, then copy payload bytes directly into the final logical-packet buffer. This removes the temporary `BinaryPacket` and payload-array allocation previously performed for every fragment.
- Reuse the fixed-size header and fragment-metadata buffers. Access is protected by the existing `_binaryReceiveLock`.
- Validate fragment index, count, total length, command, packet ID, flags, and exact frame payload length before accepting fragment data.
- Keep the receive operation serialized and close the connection if cancellation/error occurs after stream consumption, preventing reuse of a desynchronized TCP stream.
- Preserve decoded `NOT_YET_SYNCED` and `SEND_MISSING_AUTH_KEYS` responses in `PeerPacketReceived`, so the sync caller can inspect the actual response.
- Use orderly TCP close behavior rather than zero-time linger abortive close.

## Protocol compatibility

The XENO header layout, protocol version, flags, fragment size, metadata layout, and payload bytes are unchanged. This is a receiver-side implementation optimization; it does not introduce a new wire protocol.

## Validation status

The source was reviewed for consistency with the existing sender's 1,024-byte fragment payload and 16-byte fragment metadata. A .NET build/test run was not possible in this environment because the .NET SDK/MSBuild compiler is not installed. Compile and exercise both fragmented and unfragmented traffic against the project's supported target frameworks before deployment.
