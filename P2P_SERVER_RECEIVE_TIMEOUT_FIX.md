# Xenophyte P2P server receive timeout fix

## Scope

This is an incremental update to the existing server-side TCP/P2P receive path.
It does not change the XENO wire format, packet serialization, packet IDs,
fragment layout, encryption, or packet dispatch logic.

## Root cause addressed

`ClassCustomSocket.ReceiveBinaryPacketAsync` returned `null` for both:
- an inactivity timeout before any byte of a new frame was consumed; and
- terminal conditions such as cancellation, EOF, malformed data, or I/O errors.

`ClassPeerNetworkClientServerObject.HandlePeerClient` treated every `null` as
terminal and exited its receive loop. Consequently, a peer that was merely idle
for `PeerMaxDelayAwaitResponse` was disconnected before it could send a later
message.

## Changes

1. `ClassCustomSocket.LastBinaryReceiveTimedOut` reports whether the latest
   null result was specifically an inactivity timeout before consuming bytes.
2. Explicit cancellation, malformed frames, EOF, and I/O errors remain terminal.
3. A timeout after partial frame consumption still closes the socket, because
   the TCP stream position is no longer safe to reuse.
4. `HandlePeerClient` continues waiting only when the timeout was clean and the
   socket is still connected.

## Compatibility

The XENO packet format and existing packet handling are unchanged. No new
network message or protocol negotiation is introduced. Older peers remain
wire-compatible.

## Validation performed

- Source-level patch checks completed.
- Braces and parentheses counts checked in both modified C# files.
- Full compile and runtime integration tests were not run: this environment
  does not contain `dotnet`, MSBuild, `csc`, or `mcs`.

## Required regression tests before deployment

1. Keep a peer connected with no inbound packet for longer than one configured
   receive timeout; then send a valid packet and confirm it is processed.
2. Send a frame in several TCP writes with short gaps; confirm the full packet
   is reconstructed and dispatched once.
3. Pause midway through a frame beyond the inactivity timeout; confirm the
   connection is closed and the partial frame is not reused.
4. Cancel the server receive token while idle; confirm the loop exits promptly.
5. Close the remote socket gracefully while idle; confirm the server cleans up
   the peer.
6. Repeat with fragmented XENO packets and multiple concurrent peer sockets.

This change addresses an identified idle-timeout disconnect path. It does not
prove that all possible P2P message-loss causes have been eliminated.
