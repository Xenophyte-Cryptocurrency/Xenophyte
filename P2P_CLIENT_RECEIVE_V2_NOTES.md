# Xenophyte – P2P client receive improvements (V2)

## Scope

This patch improves receive-side timeout handling in `Xenophyte-Lib/Other/Object/Network/ClassCustomSocket.cs`.
It preserves the existing XENO binary frame layout, command values, fragment metadata, and serialized packet payloads.

## Change

`ReceiveBinaryPacketAsync` now tracks whether a receive operation has actually consumed bytes from the TCP stream.

- If cancellation/timeout occurs while waiting for the receive lock or for the very first TCP byte, the method returns `null` without closing the socket. The caller can retry the receive.
- If cancellation/timeout or an I/O/protocol error occurs after at least one byte has been consumed, the socket is closed. This is necessary because the next byte may be the middle of a frame and the stream can no longer be safely framed.
- The progress flag is updated by the exact-read helpers only after `NetworkStream.ReadAsync` returns a positive byte count.

## Compatibility

No wire-protocol changes. Existing peer packet serialization and XENO binary headers remain unchanged.

## Validation status

The source was reviewed for call-site consistency and the new overloads are used only by `ReceiveBinaryPacketAsync`. A .NET build and live TCP integration tests could not be run in this environment because the .NET SDK/runtime tooling is not installed.

## Recommended tests

1. Connect, send no bytes, let one receive timeout, then send a valid packet: the second receive should succeed without reconnecting.
2. Send only part of a 16-byte header, then time out: the socket should close.
3. Send a complete unfragmented packet in several TCP writes: the receiver should reconstruct it.
4. Send a valid multi-fragment packet: the receiver should reconstruct the exact original payload.
5. Cancel before the first byte and after a partial frame; verify the two different connection outcomes above.
