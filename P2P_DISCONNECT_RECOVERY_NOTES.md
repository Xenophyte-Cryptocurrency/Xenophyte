# Xenophyte P2P disconnect and synchronization stall fix

## Scope
This patch modifies two existing files only:

- `Xenophyte-Lib/Instance/Node/Network/Services/P2P/Sync/ClientSync/ClientConnect/Object/ClassPeerNetworkClientSyncObject.cs`
- `Xenophyte-Lib/Instance/Node/Network/Services/P2P/Sync/ClientSync/Service/ClassPeerNetworkSyncServiceObject.cs`

The XENO binary framing, packet command values, serialized payloads, and consensus rules are unchanged.

## Changes
- The peer receive loop catches unexpected receive/packet-processing exceptions, closes the affected socket, and completes an active response waiter with `false`.
- A request now fails promptly when the receive loop exits, rather than waiting for the entire response timeout.
- The block-batch completion counter is incremented in a `finally` block. Early `return` statements (including locked blocks) and exceptions can no longer leave the batch waiter waiting forever.
- The batch waiter uses `Interlocked`/`Volatile` for thread-safe completion counting.
- The outer block synchronization iteration catches transient exceptions, logs them to the debug output, and retries after a short delay. Cancellation still exits the loop.

## Validation
- ZIP integrity checked.
- Changes reviewed against the supplied source and existing call sites.
- No .NET SDK/MSBuild/C# compiler is available in this environment; compilation and live network tests have not been run. Build all target frameworks and test peer disconnect, locked-block early return, and temporary block-processing exceptions before production deployment.
