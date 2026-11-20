# P2P sync stall review – 20 November 2026

## Finding

`ClassPeerNetworkSyncServiceObject.StartAskBlockObjectFromListPeerTarget()` waited on a manually maintained `totalTaskDone` counter. The peer operation incremented the counter only at the end of its delegate. An exception in the range-request path (or any other exception escaping the peer delegate) skipped that increment. `TaskManager.InsertTask(..., useFactory: true)` catches and suppresses delegate exceptions, so the caller could then wait forever in `while (totalTaskDone < totalTaskToDo)` without receiving the original exception.

## Change

- Removed the redundant completion-counter polling loop. The method already awaits each `TaskManager.InsertTask` call with `useFactory: true`, which awaits the full delegate.
- Added per-peer exception logging so a failed peer request does not silently disappear.
- Added guards for null/empty peer and block-height inputs before calling `First()` / `Last()`.
- Added null guards for the range response object and its block list.
- Kept the existing packet format, peer selection, block validation, and consensus behavior unchanged.
- Changed the block-height iteration variable from `int` to `long`, matching the dictionary's key type.

## Validation

- ZIP integrity checked after packaging.
- Source inspected around the modified method and TaskManager overload. `useFactory: true` awaits the Task-returning delegate.
- No .NET SDK, MSBuild, or C# compiler is installed in this environment; compilation and live-network synchronization were not run.
