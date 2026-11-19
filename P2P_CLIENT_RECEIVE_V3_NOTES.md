# Xenophyte – P2P client continuous receive loop (V3)

## Scope

This revision changes the client-side P2P receive lifecycle in addition to the V2 socket receive safeguards. The XENO binary frame format, fragment metadata, packet serialization, and response enum values are unchanged.

## Changes

- Introduces one long-lived receive task per client connection. Only this task reads from the peer `NetworkStream`; keep-alive sends and request sends do not create competing readers.
- Creates a pending response signal before sending a request, so a fast peer response cannot arrive before the waiter is registered.
- Routes expected responses, `NOT_YET_SYNCED`, and `SEND_MISSING_AUTH_KEYS` to the current request waiter.
- Ignores unsolicited packets when no request is pending rather than allowing them to overwrite the response being consumed by the caller.
- Keeps the receive loop alive after an idle receive timeout when the socket is still open.
- Strengthens `IsConnected()` using `Socket.Poll(SelectRead)` and `Available` to detect a graceful remote close that `Socket.Connected` alone may miss.
- Waits for the previous operation's receive task to stop before starting a new reader after the per-request cancellation token is replaced.

## Compatibility

No wire-protocol changes. The XENO binary header, fragment layout, packet IDs, serialized payloads, and existing response types are preserved.

## Validation status

Static source review only. A .NET build and live peer integration tests could not be run because the .NET SDK/compiler is not installed in this environment. In particular, validate that the peer server does not send application packets asynchronously outside an active request, and verify keep-alive operation under real network latency.

## Required tests

1. A peer sends a response immediately after receiving a request; the response must not be missed.
2. A keep-alive connection remains idle across several receive timeouts, then successfully handles a new request.
3. A peer closes gracefully while the client is idle; the receive loop must exit.
4. `NOT_YET_SYNCED` and `SEND_MISSING_AUTH_KEYS` wake the current waiter and preserve their packet type.
5. Multiple requests on one keep-alive connection do not create concurrent `NetworkStream` readers.
6. Fragmented packets and partial-frame timeout behavior remain identical to V2.
