# Xenophyte Sync Engine V2 — preliminary patch

## Changes
- Executes peer block requests in bounded batches of four instead of awaiting each peer sequentially.
- Replaces the manual completion counter and polling delay with `Task.WhenAll` over the actual peer tasks.
- Uses a concurrent per-height peer dictionary to avoid concurrent writes to a normal `Dictionary`.
- Uses `TryAdd` for peer responses so duplicate responses cannot throw during collection.
- Adds empty-input guards before `First()` / `Last()` and validates range response containers.
- Iterates block heights as `long`, matching the outer dictionary key type.

## Compatibility / safety
- Does not change packet formats, block serialization, consensus rules, peer selection, or vote selection.
- Changes are isolated to `StartAskBlockObjectFromListPeerTarget`.
- This is a candidate patch only: build, unit tests, and live-network tests are still required.

## Build status
Not compiled in this environment: the supplied SDK package depends on the .NET 9 runtime, apphost and targeting packs, and network access to retrieve those dependencies is unavailable.
