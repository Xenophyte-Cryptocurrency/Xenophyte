# Xenophyte Sync Engine V2 — validation report

Date: 2026-11-21

## Static checks

- ZIP source archive extracted successfully.
- Candidate source patch applied to a separate working copy.
- Focused method inspection confirms bounded batches of four peer tasks.
- `Task.WhenAll` joins each batch before block aggregation begins.
- Manual completion counter and polling loop removed from the target method.
- Per-height peer buckets use `ConcurrentDictionary<int, ClassBlockObject>`.
- Block-height key enumeration uses `long`.
- Empty peer/block input and null response container guards are present.
- Output archive integrity check passed.

## Build and runtime tests

Not run. The supplied `dotnet-sdk-9.0` Debian package declares dependencies on `dotnet-runtime-9.0`, `dotnet-targeting-pack-9.0`, `dotnet-apphost-pack-9.0`, `netstandard-targeting-pack-2.1`, `aspnetcore-runtime-9.0`, and `aspnetcore-targeting-pack-9.0`. The sandbox has no installed .NET host/runtime and outbound package repository access timed out. Therefore no claim of successful compilation or runtime correctness is made.

## Next validation required

1. Install the declared .NET 9 dependencies and SDK in a network-enabled Linux build environment.
2. Restore NuGet dependencies.
3. Build `Xenophyte-Lib` for `net9.0` first.
4. Build the remaining Linux-compatible projects.
5. Add deterministic tests for parallel peer replies, duplicate block responses, peer exceptions, cancellation, and empty inputs.
6. Run controlled multi-peer synchronization benchmarks before considering deployment.

## Scope

Only `StartAskBlockObjectFromListPeerTarget` was modified. Packet framing, block serialization, peer selection, consensus validation, and vote selection were not intentionally changed.
