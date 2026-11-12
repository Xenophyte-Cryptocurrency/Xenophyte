using Newtonsoft.Json;
using SeguraChain_Lib.Blockchain.Database;
using SeguraChain_Lib.Blockchain.MemPool.Database;
using SeguraChain_Lib.Blockchain.Transaction.Enum;
using SeguraChain_Lib.Blockchain.Transaction.Utility;
using SeguraChain_Lib.Instance.Node.Network.Enum.P2P.Packet;
using SeguraChain_Lib.Instance.Node.Network.Services.P2P.Sync.ClientSync.Function;
using SeguraChain_Lib.Instance.Node.Network.Services.P2P.Sync.Packet;
using SeguraChain_Lib.Instance.Node.Network.Services.P2P.Sync.Packet.SubPacket.Request;
using SeguraChain_Lib.Instance.Node.Network.Services.P2P.Sync.Packet.SubPacket.Response;
using SeguraChain_Lib.Instance.Node.Setting.Object;
using SeguraChain_Lib.Utility;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using SeguraChain_Lib.Log;
using SeguraChain_Lib.Instance.Node.Network.Database;
using SeguraChain_Lib.Blockchain.Transaction.Object;
using SeguraChain_Lib.Instance.Node.Network.Database.Manager;
using SeguraChain_Lib.Other.Object.List;
using System.Diagnostics;
using SeguraChain_Lib.Instance.Node.Network.Database.Object;
using System.Collections.Concurrent;
using SeguraChain_Lib.Instance.Node.Network.Services.P2P.Sync.ClientSync.ClientConnect.Object;
using SeguraChain_Lib.Blockchain.Wallet.Function;

namespace SeguraChain_Lib.Instance.Node.Network.Services.P2P.Broadcast
{
    /// <summary>
    /// Manages one receiver and (on public nodes) one sender mempool broadcast client per public peer.
    /// 
    /// Fixes compared to the original version:
    ///  - thread-safe collections (ConcurrentDictionary) instead of Dictionary;
    ///  - dead sender instances are now removed and can be recreated;
    ///  - the sender instance is no longer created only when the receiver was just created;
    ///  - null checks on peer objects, signed packets and received transaction lists;
    ///  - already sent transactions are no longer re-sent every cycle (and old heights are purged);
    ///  - correct IDisposable pattern;
    ///  - exceptions are logged instead of silently swallowed, and async void tasks cannot crash the process;
    ///  - full transaction JSON only logged in DEBUG.
    /// </summary>
    public class ClassPeerNetworkBroadcastInstanceMemPool
    {
        private ClassPeerDatabase _peerDatabase;
        private ConcurrentDictionary<string, ConcurrentDictionary<string, ClassPeerNetworkClientBroadcastMemPool>> _listPeerNetworkClientBroadcastMemPoolReceiver; // Peer IP | [Peer Unique ID | Client broadcast object]
        private ConcurrentDictionary<string, ConcurrentDictionary<string, ClassPeerNetworkClientBroadcastMemPool>> _listPeerNetworkClientBroadcastMemPoolSender; // Peer IP | [Peer Unique ID | Client broadcast object]
        private ClassPeerNetworkSettingObject _peerNetworkSettingObject;
        private ClassPeerFirewallSettingObject _peerFirewallSettingObject;
        private CancellationTokenSource _cancellation;
        private string _peerOpenNatIp;
        private volatile bool _peerNetworkBroadcastInstanceMemPoolStatus;

        /// <summary>
        /// Constructor.
        /// </summary>
        public ClassPeerNetworkBroadcastInstanceMemPool(ClassPeerDatabase peerDatabase, ClassPeerNetworkSettingObject peerNetworkSettingObject, ClassPeerFirewallSettingObject peerFirewallSettingObject, string peerOpenNatIp)
        {
            _peerDatabase = peerDatabase;
            _listPeerNetworkClientBroadcastMemPoolReceiver = new ConcurrentDictionary<string, ConcurrentDictionary<string, ClassPeerNetworkClientBroadcastMemPool>>();
            _listPeerNetworkClientBroadcastMemPoolSender = new ConcurrentDictionary<string, ConcurrentDictionary<string, ClassPeerNetworkClientBroadcastMemPool>>();
            _peerNetworkSettingObject = peerNetworkSettingObject;
            _peerFirewallSettingObject = peerFirewallSettingObject;
            _peerOpenNatIp = peerOpenNatIp;
        }

        private static void Log(string message, ConsoleColor color)
        {
            Debug.WriteLine(message); // Only compiled in DEBUG builds (Conditional attribute).
            ClassLog.WriteLine(message, ClassEnumLogLevelType.LOG_LEVEL_MEMPOOL_BROADCAST, ClassEnumLogWriteLevel.LOG_WRITE_LEVEL_MANDATORY_PRIORITY, false, color);
        }

        #region Run / Stop

        /// <summary>
        /// Run the network broadcast mempool instance.
        /// </summary>
        public void RunNetworkBroadcastMemPoolInstanceTask()
        {
            _cancellation = new CancellationTokenSource();
            _peerNetworkBroadcastInstanceMemPoolStatus = true;

            TaskManager.TaskManager.InsertTask(new Action(async () =>
            {
                try
                {
                    while (_peerNetworkBroadcastInstanceMemPoolStatus && !_cancellation.IsCancellationRequested)
                    {
                        try
                        {
                            GenerateBroadcastInstances();
                            CleanDeadInstances(_listPeerNetworkClientBroadcastMemPoolReceiver, "receiver");
                            CleanDeadInstances(_listPeerNetworkClientBroadcastMemPoolSender, "sender");
                        }
                        catch (Exception error)
                        {
                            Log("Error on generating MemPool instance. Exception: " + error.Message, ConsoleColor.Red);
                        }

                        try
                        {
                            await Task.Delay(1000, _cancellation.Token);
                        }
                        catch
                        {
                            // Cancelled or disposed: the loop condition will stop the task.
                            break;
                        }
                    }
                }
                catch (Exception error)
                {
                    Log("MemPool broadcast instance task stopped on exception: " + error.Message, ConsoleColor.Red);
                }
            }), 0, _cancellation, null).Wait();
        }

        /// <summary>
        /// Stop the network broadcast mempool instance.
        /// </summary>
        public void StopNetworkBroadcastMemPoolInstance()
        {
            _peerNetworkBroadcastInstanceMemPoolStatus = false;

            try
            {
                if (_cancellation != null && !_cancellation.IsCancellationRequested)
                    _cancellation.Cancel();
            }
            catch
            {
                // Ignored.
            }

            StopAndClearInstances(_listPeerNetworkClientBroadcastMemPoolReceiver);
            StopAndClearInstances(_listPeerNetworkClientBroadcastMemPoolSender);
        }

        private static void StopAndClearInstances(ConcurrentDictionary<string, ConcurrentDictionary<string, ClassPeerNetworkClientBroadcastMemPool>> list)
        {
            foreach (string peerIp in list.Keys.ToArray())
            {
                if (!list.TryRemove(peerIp, out ConcurrentDictionary<string, ClassPeerNetworkClientBroadcastMemPool> clients) || clients == null)
                    continue;

                foreach (ClassPeerNetworkClientBroadcastMemPool client in clients.Values.ToArray())
                {
                    try
                    {
                        client.StopTaskAndDisconnect();
                    }
                    catch
                    {
                        // Ignored.
                    }
                }

                clients.Clear();
            }
        }

        #endregion

        #region Instances management

        /// <summary>
        /// Generate sender/receiver broadcast instances by targeting public peers.
        /// </summary>
        private void GenerateBroadcastInstances()
        {
            if (_peerDatabase.Count == 0)
                return;

            foreach (string peerIpTarget in _peerDatabase.Keys.ToArray())
            {
                if (_cancellation.IsCancellationRequested)
                    return;

                if (peerIpTarget.IsNullOrEmpty(false, out _))
                    continue;

                try
                {
                    ConcurrentDictionary<string, ClassPeerObject> peerListObject = _peerDatabase[peerIpTarget, _cancellation];

                    if (peerListObject == null)
                        continue;

                    foreach (string peerUniqueIdTarget in peerListObject.Keys.ToArray())
                    {
                        if (_cancellation.IsCancellationRequested)
                            return;

                        if (peerUniqueIdTarget.IsNullOrEmpty(false, out _))
                            continue;

                        ClassPeerObject peerObject = _peerDatabase[peerIpTarget, peerUniqueIdTarget, _cancellation];

                        // FIX: the peer can be removed between the key listing and this read.
                        if (peerObject == null || !peerObject.PeerIsPublic)
                            continue;

                        // The receiver is required first.
                        if (!EnsureClientInstance(_listPeerNetworkClientBroadcastMemPoolReceiver, peerIpTarget, peerUniqueIdTarget, false))
                            continue;

                        // FIX: the sender is now built whenever it is missing (not only when the receiver was just created),
                        // and only if this node is a public node, as the original comment says.
                        if (_peerNetworkSettingObject.PublicPeer)
                            EnsureClientInstance(_listPeerNetworkClientBroadcastMemPoolSender, peerIpTarget, peerUniqueIdTarget, true);
                    }
                }
                catch (Exception error)
                {
                    // FIX: a failure on one peer must not prevent the others from being processed.
                    Log("Error on generating MemPool instance for peer " + peerIpTarget + ". Exception: " + error.Message, ConsoleColor.Red);
                }
            }
        }

        /// <summary>
        /// Create and start a client instance if it does not exist yet.
        /// </summary>
        /// <returns>True if an instance exists (already or newly started) for this peer.</returns>
        private bool EnsureClientInstance(ConcurrentDictionary<string, ConcurrentDictionary<string, ClassPeerNetworkClientBroadcastMemPool>> list, string peerIp, string peerUniqueId, bool onSendingMode)
        {
            ConcurrentDictionary<string, ClassPeerNetworkClientBroadcastMemPool> clientsOfPeerIp = list.GetOrAdd(peerIp, _ => new ConcurrentDictionary<string, ClassPeerNetworkClientBroadcastMemPool>());

            if (clientsOfPeerIp.ContainsKey(peerUniqueId))
                return true;

            ClassPeerNetworkClientBroadcastMemPool client = new ClassPeerNetworkClientBroadcastMemPool(
                _peerDatabase,
                _cancellation,
                peerIp,
                peerUniqueId,
                onSendingMode,
                _peerNetworkSettingObject,
                _peerFirewallSettingObject);

            if (!clientsOfPeerIp.TryAdd(peerUniqueId, client))
            {
                client.StopTaskAndDisconnect();
                return true; // Added concurrently by someone else.
            }

            bool success = RunPeerNetworkClientBroadcastMemPool(client);

            if (!success)
            {
                client.StopTaskAndDisconnect();
                clientsOfPeerIp.TryRemove(peerUniqueId, out _);
            }

            Log("Run Sync MemPool " + (onSendingMode ? "Sending" : "Receive") + " Mode transaction from Peer: " + peerIp + ":" + peerUniqueId + " | Status: " + (success ? "success" : "failed") + ".", success ? ConsoleColor.Green : ConsoleColor.Red);

            return success;
        }

        /// <summary>
        /// Run a peer network client broadcast mempool.
        /// </summary>
        private bool RunPeerNetworkClientBroadcastMemPool(ClassPeerNetworkClientBroadcastMemPool client)
        {
            try
            {
                client.RunBroadcastTransactionTask();
            }
            catch (Exception error)
            {
                Log("Unable to run the MemPool broadcast client. Exception: " + error.Message, ConsoleColor.Red);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Remove the dead instances (receiver and sender) so they can be rebuilt on the next cycle.
        /// </summary>
        private void CleanDeadInstances(ConcurrentDictionary<string, ConcurrentDictionary<string, ClassPeerNetworkClientBroadcastMemPool>> list, string name)
        {
            foreach (string peerIp in list.Keys.ToArray())
            {
                if (!list.TryGetValue(peerIp, out ConcurrentDictionary<string, ClassPeerNetworkClientBroadcastMemPool> clients))
                    continue;

                foreach (string peerUniqueId in clients.Keys.ToArray())
                {
                    if (!clients.TryGetValue(peerUniqueId, out ClassPeerNetworkClientBroadcastMemPool client))
                        continue;

                    if (client.IsAlive)
                        continue;

                    try
                    {
                        client.StopTaskAndDisconnect();
                    }
                    catch
                    {
                        // Ignored.
                    }

                    // FIX: the original code never removed dead senders, so they were never rebuilt.
                    clients.TryRemove(peerUniqueId, out _);

                    Debug.WriteLine("MemPool Broadcast instance " + name + " of peer " + peerIp + ":" + peerUniqueId + " is dead.");
                }

                if (clients.IsEmpty)
                    list.TryRemove(peerIp, out _);
            }
        }

        #endregion

        /// <summary>
        /// Peer broadcast client mempool transaction object.
        /// </summary>
        internal class ClassPeerNetworkClientBroadcastMemPool : ClassPeerSyncFunction, IDisposable
        {
            private ClassPeerDatabase _peerDatabase;
            public volatile bool IsAlive;
            private string _peerIpTarget;
            private string _peerUniqueIdTarget;
            private ClassPeerNetworkSettingObject _peerNetworkSettingObject;
            private ClassPeerFirewallSettingObject _peerFirewallSettingObject;
            private CancellationTokenSource _peerCancellationToken;
            private ConcurrentDictionary<long, ConcurrentDictionary<string, byte>> _memPoolListBlockHeightTransactionReceived; // Block height | [Tx hash | unused]
            private ConcurrentDictionary<long, ConcurrentDictionary<string, byte>> _memPoolListBlockHeightTransactionSend; // Block height | [Tx hash | unused]
            private bool _onSendBroadcastMode;

            /// <summary>
            /// Constructor.
            /// </summary>
            public ClassPeerNetworkClientBroadcastMemPool(ClassPeerDatabase peerDatabase, CancellationTokenSource peerCancellation, string peerIpTarget, string peerUniqueIdTarget, bool onSendBroadcastMode, ClassPeerNetworkSettingObject peerNetworkSettingObject, ClassPeerFirewallSettingObject peerFirewallSettingObject)
            {
                _peerDatabase = peerDatabase;
                _peerCancellationToken = CancellationTokenSource.CreateLinkedTokenSource(peerCancellation.Token);
                _peerIpTarget = peerIpTarget;
                _peerUniqueIdTarget = peerUniqueIdTarget;
                _onSendBroadcastMode = onSendBroadcastMode;
                _peerNetworkSettingObject = peerNetworkSettingObject;
                _peerFirewallSettingObject = peerFirewallSettingObject;
                _memPoolListBlockHeightTransactionReceived = new ConcurrentDictionary<long, ConcurrentDictionary<string, byte>>();
                _memPoolListBlockHeightTransactionSend = new ConcurrentDictionary<long, ConcurrentDictionary<string, byte>>();
            }

            #region Dispose functions

            private bool _disposed;

            ~ClassPeerNetworkClientBroadcastMemPool()
            {
                // FIX: a finalizer must not touch managed objects (disposing = false).
                Dispose(false);
            }

            public void Dispose()
            {
                Dispose(true);
                GC.SuppressFinalize(this);
            }

            protected virtual void Dispose(bool disposing)
            {
                // FIX: the original condition (_disposed && !IsAlive) could run the cleanup several times.
                if (_disposed)
                    return;

                _disposed = true;

                if (disposing)
                    StopTaskAndDisconnect();
            }

            #endregion

            #region Initialize/Stop broadcast client.

            private bool _stopped;

            /// <summary>
            /// Stop tasks and disconnect.
            /// </summary>
            public void StopTaskAndDisconnect()
            {
                IsAlive = false;

                // Stop tasks (only once).
                if (!_stopped)
                {
                    _stopped = true;

                    if (_peerCancellationToken != null)
                    {
                        try
                        {
                            if (!_peerCancellationToken.IsCancellationRequested)
                                _peerCancellationToken.Cancel();
                        }
                        catch
                        {
                            // Ignored.
                        }

                        try
                        {
                            _peerCancellationToken.Dispose();
                        }
                        catch
                        {
                            // Ignored.
                        }
                    }
                }

                // Clean up past heights received/sent (the collections are thread-safe).
                _memPoolListBlockHeightTransactionReceived?.Clear();
                _memPoolListBlockHeightTransactionSend?.Clear();
            }

            #endregion

            private static void Log(string message, ConsoleColor color)
            {
                Debug.WriteLine(message); // Only compiled in DEBUG builds (Conditional attribute).
                ClassLog.WriteLine(message, ClassEnumLogLevelType.LOG_LEVEL_MEMPOOL_BROADCAST, ClassEnumLogWriteLevel.LOG_WRITE_LEVEL_MANDATORY_PRIORITY, false, color);
            }

            /// <summary>
            /// Wait the configured sync delay. Returns false if the task has been cancelled.
            /// </summary>
            private async Task<bool> WaitSyncDelayAsync()
            {
                try
                {
                    await Task.Delay(_peerNetworkSettingObject.PeerTaskSyncDelay, _peerCancellationToken.Token);
                    return true;
                }
                catch
                {
                    // Cancelled or disposed.
                    return false;
                }
            }

            #region Broadcast tasks.

            /// <summary>
            /// Run the broadcast mempool client transaction tasks.
            /// </summary>
            public void RunBroadcastTransactionTask()
            {
                IsAlive = true;

                if (!_onSendBroadcastMode)
                    EnableReceiveBroadcastTransactionTask();
                else
                    EnableSendBroadcastTransactionTask();
            }

            /// <summary>
            /// Enable the received broadcast transaction task.
            /// </summary>
            private void EnableReceiveBroadcastTransactionTask()
            {
                TaskManager.TaskManager.InsertTask(new Action(async () =>
                {
                    try
                    {
                        using (DisposableDictionary<string, string> listWalletAddressAndPublicKeyCache = new DisposableDictionary<string, string>())
                        {
                            while (IsAlive && !_peerCancellationToken.IsCancellationRequested)
                            {
                                try
                                {
                                    if (!await ReceiveCycleAsync(listWalletAddressAndPublicKeyCache))
                                    {
                                        IsAlive = false;
                                        break;
                                    }
                                }
                                catch (OperationCanceledException)
                                {
                                    break;
                                }
                                catch (Exception error)
                                {
                                    Log("Task to receive MemPool transaction failed from: " + _peerIpTarget + " | Exception. Result: " + error.Message, ConsoleColor.Yellow);
                                }

                                if (!await WaitSyncDelayAsync())
                                    break;
                            }
                        }
                    }
                    catch (Exception error)
                    {
                        // An exception escaping an async void delegate would crash the process.
                        IsAlive = false;
                        Log("Receive MemPool task stopped on exception from: " + _peerIpTarget + " | " + error.Message, ConsoleColor.Red);
                    }
                }), 0, _peerCancellationToken).Wait();

                Debug.WriteLine("Enable Receive Broadcast transaction MemPool from: " + _peerIpTarget + " done. " + IsAlive);
            }

            /// <summary>
            /// One receive cycle. Returns false if the peer must be considered as dead.
            /// </summary>
            private async Task<bool> ReceiveCycleAsync(DisposableDictionary<string, string> listWalletAddressAndPublicKeyCache)
            {
                ClassPeerObject peerObject = _peerDatabase[_peerIpTarget, _peerUniqueIdTarget, _peerCancellationToken];

                if (peerObject == null)
                    return false;

                long lastBlockHeightUnlocked = await ClassBlockchainDatabase.BlockchainMemoryManagement.GetLastBlockHeightUnlocked(_peerCancellationToken);

                // Clean passed blocks mined.
                foreach (long blockHeight in _memPoolListBlockHeightTransactionReceived.Keys.ToArray())
                {
                    if (blockHeight <= lastBlockHeightUnlocked)
                        _memPoolListBlockHeightTransactionReceived.TryRemove(blockHeight, out _);
                }

                #region First ask the mem pool block height list and their transaction counts.

                ClassPeerPacketSendObject packetSendObject = new ClassPeerPacketSendObject(_peerNetworkSettingObject.PeerUniqueId,
                    peerObject.PeerInternPublicKey,
                    peerObject.PeerClientLastTimestampPeerPacketSignatureWhitelist)
                {
                    PacketOrder = ClassPeerEnumPacketSend.ASK_MEM_POOL_BLOCK_HEIGHT_LIST_BROADCAST_MODE,
                    PacketContent = ClassUtility.SerializeData(new ClassPeerPacketSendAskMemPoolBlockHeightList()
                    {
                        PacketTimestamp = TaskManager.TaskManager.CurrentTimestampSecond
                    }),
                };

                packetSendObject = await ClassPeerNetworkBroadcastShortcutFunction.BuildSignedPeerSendPacketObject(_peerDatabase, packetSendObject, _peerIpTarget, _peerUniqueIdTarget, false, _peerNetworkSettingObject, _peerCancellationToken);

                if (packetSendObject == null)
                    return false;

                ClassPeerNetworkClientSyncObject peerNetworkClientSyncObject = new ClassPeerNetworkClientSyncObject(_peerDatabase, _peerIpTarget, peerObject.PeerPort, _peerUniqueIdTarget, _peerNetworkSettingObject, _peerFirewallSettingObject);

                Log("Try to send ask MemPool block height list: " + _peerIpTarget + " request.", ConsoleColor.Yellow);

                if (!await peerNetworkClientSyncObject.TrySendPacketToPeerTarget(packetSendObject, true, peerObject.PeerPort, _peerUniqueIdTarget, _peerCancellationToken, ClassPeerEnumPacketResponse.SEND_MEM_POOL_BLOCK_HEIGHT_LIST_BROADCAST_MODE, false, false))
                {
                    Log("Try to send ask MemPool block height list: " + _peerIpTarget + " failed.", ConsoleColor.Yellow);
                    return false;
                }

                if (peerNetworkClientSyncObject.PeerPacketReceived == null)
                    return false;

                ClassTranslatePacket<ClassPeerPacketSendMemPoolBlockHeightList> peerPacketMemPoolBlockHeightListTranslated = await TranslatePacketReceived<ClassPeerPacketSendMemPoolBlockHeightList>(peerNetworkClientSyncObject.PeerPacketReceived, ClassPeerEnumPacketResponse.SEND_MEM_POOL_BLOCK_HEIGHT_LIST_BROADCAST_MODE, _peerCancellationToken);

                // FIX: check the status/null before dereferencing PacketTranslated.
                if (peerPacketMemPoolBlockHeightListTranslated == null || !peerPacketMemPoolBlockHeightListTranslated.Status || peerPacketMemPoolBlockHeightListTranslated.PacketTranslated == null)
                    return false;

                var memPoolBlockHeightListAndCount = peerPacketMemPoolBlockHeightListTranslated.PacketTranslated.MemPoolBlockHeightListAndCount;

                if (memPoolBlockHeightListAndCount == null)
                    return false;

                Log("Request to ask MemPool block height list: " + _peerIpTarget + " done. Result: " + JsonConvert.SerializeObject(memPoolBlockHeightListAndCount), ConsoleColor.Yellow);

                #endregion

                #region Then sync transaction by height.

                if (memPoolBlockHeightListAndCount.Count == 0)
                    return true;

                foreach (long blockHeight in memPoolBlockHeightListAndCount.Keys.OrderBy(x => x))
                {
                    if (_peerCancellationToken.IsCancellationRequested)
                        break;

                    if (ClassBlockchainDatabase.BlockchainMemoryManagement.GetLastBlockHeight >= blockHeight)
                        continue;

                    int countToSync = memPoolBlockHeightListAndCount[blockHeight];

                    if (countToSync <= 0)
                        continue;

                    long lastBlockHeightUnlockedConfirmed = await ClassBlockchainDatabase.BlockchainMemoryManagement.GetLastBlockHeightTransactionConfirmationDone(_peerCancellationToken);

                    if (lastBlockHeightUnlockedConfirmed == blockHeight)
                        continue;

                    int countAlreadySynced = 0;

                    if (_memPoolListBlockHeightTransactionReceived.TryGetValue(blockHeight, out ConcurrentDictionary<string, byte> alreadyReceived))
                        countAlreadySynced = alreadyReceived.Count;

                    if (countToSync == countAlreadySynced)
                        continue;

                    alreadyReceived = _memPoolListBlockHeightTransactionReceived.GetOrAdd(blockHeight, _ => new ConcurrentDictionary<string, byte>());

                    // Ensure to be compatible with most recent transactions sent.
                    packetSendObject = new ClassPeerPacketSendObject(_peerNetworkSettingObject.PeerUniqueId,
                        peerObject.PeerInternPublicKey,
                        peerObject.PeerClientLastTimestampPeerPacketSignatureWhitelist)
                    {
                        PacketOrder = ClassPeerEnumPacketSend.ASK_MEM_POOL_TRANSACTION_BY_BLOCK_HEIGHT_BROADCAST_MODE,
                        PacketContent = ClassUtility.SerializeData(new ClassPeerPacketSendAskMemPoolTransactionList()
                        {
                            BlockHeight = blockHeight,
                            // TODO: if the remote peer paginates its answer, this must be advanced to fetch the next pages.
                            TotalTransactionProgress = 0,
                            PacketTimestamp = TaskManager.TaskManager.CurrentTimestampSecond
                        }),
                    };

                    packetSendObject = await ClassPeerNetworkBroadcastShortcutFunction.BuildSignedPeerSendPacketObject(_peerDatabase, packetSendObject, _peerIpTarget, _peerUniqueIdTarget, false, _peerNetworkSettingObject, _peerCancellationToken);

                    // FIX: the original code did not check this and could throw a NullReferenceException.
                    if (packetSendObject == null)
                        return false;

                    // NOTE: the expected response of the send call (..._END_TRANSACTION_BY_BLOCK_HEIGHT...) differs from the one
                    // used by the translation below (..._TRANSACTION_BY_BLOCK_HEIGHT...). Kept as in the original code: verify
                    // against the packet protocol that this is intended.
                    if (!await peerNetworkClientSyncObject.TrySendPacketToPeerTarget(packetSendObject, true, peerObject.PeerPort, _peerUniqueIdTarget, _peerCancellationToken, ClassPeerEnumPacketResponse.SEND_MEM_POOL_END_TRANSACTION_BY_BLOCK_HEIGHT_BROADCAST_MODE, false, false))
                        return false;

                    ClassTranslatePacket<ClassPeerPacketSendMemPoolTransaction> packetTranslated = await TranslatePacketReceived<ClassPeerPacketSendMemPoolTransaction>(peerNetworkClientSyncObject.PeerPacketReceived, ClassPeerEnumPacketResponse.SEND_MEM_POOL_TRANSACTION_BY_BLOCK_HEIGHT_BROADCAST_MODE, _peerCancellationToken);

                    // FIX: ListTransactionObject comes from a remote peer and can be null.
                    if (packetTranslated == null || !packetTranslated.Status || packetTranslated.PacketTranslated == null || packetTranslated.PacketTranslated.ListTransactionObject == null)
                        continue;

                    foreach (ClassTransactionObject transactionObject in packetTranslated.PacketTranslated.ListTransactionObject)
                    {
                        if (_peerCancellationToken.IsCancellationRequested)
                            break;

                        if (transactionObject == null || string.IsNullOrEmpty(transactionObject.TransactionHash))
                            continue;

#if DEBUG
                        Debug.WriteLine("Transaction object received from peer: " + _peerIpTarget + " | Data: " + JsonConvert.SerializeObject(transactionObject));
#endif
                        Log("Transaction received from peer: " + _peerIpTarget + " | Hash: " + transactionObject.TransactionHash, ConsoleColor.Yellow);

                        if (transactionObject.TransactionType == ClassTransactionEnumType.BLOCK_REWARD_TRANSACTION ||
                            transactionObject.TransactionType == ClassTransactionEnumType.DEV_FEE_TRANSACTION)
                            continue;

                        if (alreadyReceived.ContainsKey(transactionObject.TransactionHash))
                            continue;

                        if (transactionObject.BlockHeightTransaction <= ClassBlockchainDatabase.BlockchainMemoryManagement.GetLastBlockHeight)
                            continue;

                        ClassTransactionEnumStatus checkTxResult = await ClassTransactionUtility.CheckTransactionWithBlockchainData(transactionObject, true, false, true, null, 0, listWalletAddressAndPublicKeyCache, true, _peerCancellationToken);

                        if (checkTxResult == ClassTransactionEnumStatus.VALID_TRANSACTION || checkTxResult == ClassTransactionEnumStatus.DUPLICATE_TRANSACTION_HASH)
                        {
                            ClassMemPoolDatabase.InsertTxToMemPool(transactionObject);
                            alreadyReceived.TryAdd(transactionObject.TransactionHash, 0);
                        }
                    }
                }

                #endregion

                return true;
            }

            /// <summary>
            /// Enable the send broadcast transaction task.
            /// </summary>
            private void EnableSendBroadcastTransactionTask()
            {
                TaskManager.TaskManager.InsertTask(new Action(async () =>
                {
                    try
                    {
                        while (IsAlive && !_peerCancellationToken.IsCancellationRequested)
                        {
                            try
                            {
                                if (!await SendCycleAsync())
                                {
                                    IsAlive = false;
                                    break;
                                }
                            }
                            catch (OperationCanceledException)
                            {
                                IsAlive = false;
                                break;
                            }
                            catch (Exception error)
                            {
                                // FIX: the exception is now logged instead of being silently swallowed.
                                Log("Task to send MemPool transaction failed to: " + _peerIpTarget + " | Exception. Result: " + error.Message, ConsoleColor.Yellow);
                                IsAlive = false;
                                break;
                            }

                            if (!await WaitSyncDelayAsync())
                                break;
                        }
                    }
                    catch (Exception error)
                    {
                        IsAlive = false;
                        Log("Send MemPool task stopped on exception to: " + _peerIpTarget + " | " + error.Message, ConsoleColor.Red);
                    }
                }), 0, _peerCancellationToken).Wait();

                Debug.WriteLine("Enable Send Broadcast transaction MemPool from: " + _peerIpTarget + " done.");
            }

            /// <summary>
            /// One send cycle. Returns false if the peer must be considered as dead.
            /// </summary>
            private async Task<bool> SendCycleAsync()
            {
                using (DisposableList<long> listMemPoolBlockHeight = await ClassMemPoolDatabase.GetMemPoolListBlockHeight(_peerCancellationToken))
                {
                    HashSet<long> currentHeights = new HashSet<long>(listMemPoolBlockHeight.GetList);

                    // FIX: purge the heights that are no longer in the mempool (memory leak in the original code).
                    foreach (long sentHeight in _memPoolListBlockHeightTransactionSend.Keys.ToArray())
                    {
                        if (!currentHeights.Contains(sentHeight))
                            _memPoolListBlockHeightTransactionSend.TryRemove(sentHeight, out _);
                    }

                    foreach (long blockHeight in currentHeights.OrderBy(x => x))
                    {
                        if (_peerCancellationToken.IsCancellationRequested)
                            break;

                        int countMemPoolTransaction = await ClassMemPoolDatabase.GetCountMemPoolTxFromBlockHeight(blockHeight, true, _peerCancellationToken);

                        if (countMemPoolTransaction <= 0)
                            continue;

                        ConcurrentDictionary<string, byte> alreadySent = _memPoolListBlockHeightTransactionSend.GetOrAdd(blockHeight, _ => new ConcurrentDictionary<string, byte>());

                        using (DisposableList<ClassTransactionObject> listTransactionObjectInMemPool = await ClassMemPoolDatabase.GetMemPoolTxObjectFromBlockHeight(blockHeight, true, _peerCancellationToken))
                        {
                            // FIX: the original code recorded the sent hashes but never used them to filter the next sends.
                            List<ClassTransactionObject> listTransactionObjectToSend = listTransactionObjectInMemPool.GetList
                                .Where(tx => tx != null && !alreadySent.ContainsKey(tx.TransactionHash))
                                .ToList();

                            if (listTransactionObjectToSend.Count == 0)
                                continue;

                            // FIX: read the peer once and check it (it can be removed from the database at any time).
                            ClassPeerObject peerObject = _peerDatabase[_peerIpTarget, _peerUniqueIdTarget, _peerCancellationToken];

                            if (peerObject == null)
                                return false;

                            ClassPeerPacketSendObject packetSendObject = new ClassPeerPacketSendObject(_peerNetworkSettingObject.PeerUniqueId, peerObject.PeerInternPublicKey, peerObject.PeerClientLastTimestampPeerPacketSignatureWhitelist)
                            {
                                PacketOrder = ClassPeerEnumPacketSend.ASK_MEM_POOL_TRANSACTION_VOTE,
                                PacketContent = ClassUtility.SerializeData(new ClassPeerPacketSendAskMemPoolTransactionVote()
                                {
                                    ListTransactionObject = listTransactionObjectToSend,
                                    PacketTimestamp = TaskManager.TaskManager.CurrentTimestampSecond,
                                })
                            };

                            packetSendObject = await ClassPeerNetworkBroadcastShortcutFunction.BuildSignedPeerSendPacketObject(_peerDatabase, packetSendObject, _peerIpTarget, _peerUniqueIdTarget, false, _peerNetworkSettingObject, _peerCancellationToken);

                            // FIX: the original code did not check this and could throw a NullReferenceException.
                            if (packetSendObject == null)
                                return false;

                            ClassPeerNetworkClientSyncObject peerNetworkClientSyncObject = new ClassPeerNetworkClientSyncObject(_peerDatabase, _peerIpTarget, peerObject.PeerPort, _peerUniqueIdTarget, _peerNetworkSettingObject, _peerFirewallSettingObject);

                            if (!await peerNetworkClientSyncObject.TrySendPacketToPeerTarget(packetSendObject, true, peerObject.PeerPort, _peerUniqueIdTarget, _peerCancellationToken, ClassPeerEnumPacketResponse.SEND_MEM_POOL_TRANSACTION_VOTE, true, false))
                                return false;

                            ClassTranslatePacket<ClassPeerPacketSendMemPoolTransactionVote> packetTranslated = await TranslatePacketReceived<ClassPeerPacketSendMemPoolTransactionVote>(peerNetworkClientSyncObject.PeerPacketReceived, ClassPeerEnumPacketResponse.SEND_MEM_POOL_TRANSACTION_VOTE, _peerCancellationToken);

                            if (packetTranslated != null && packetTranslated.Status)
                            {
                                foreach (ClassTransactionObject transactionObject in listTransactionObjectToSend)
                                    alreadySent.TryAdd(transactionObject.TransactionHash, 0);
                            }
                        }
                    }
                }

                return true;
            }

            #endregion

            #region Packet management.

            /// <summary>
            /// Translate the packet received.
            /// </summary>
            private async Task<ClassTranslatePacket<T>> TranslatePacketReceived<T>(ClassPeerPacketRecvObject peerPacketRecvObject, ClassPeerEnumPacketResponse packetResponseExpected, CancellationTokenSource cancellation)
            {
                ClassTranslatePacket<T> translatedPacket = new ClassTranslatePacket<T>();

                if (peerPacketRecvObject == null || peerPacketRecvObject.PacketContent.IsNullOrEmpty(false, out _))
                    return translatedPacket;

                try
                {
                    // FIX: the peer can disappear from the database: avoid a NullReferenceException.
                    ClassPeerObject peerObject = _peerDatabase[_peerIpTarget, _peerUniqueIdTarget, _peerCancellationToken];

                    if (peerObject == null)
                        return translatedPacket;

                    bool peerIgnorePacketSignature = ClassPeerCheckManager.CheckPeerClientWhitelistStatus(_peerDatabase, _peerIpTarget, _peerUniqueIdTarget, _peerNetworkSettingObject, cancellation);

                    if (!peerIgnorePacketSignature)
                        peerIgnorePacketSignature = peerObject.PeerTimestampSignatureWhitelist >= TaskManager.TaskManager.CurrentTimestampSecond;

                    bool packetSignatureStatus = true;

                    if (!peerIgnorePacketSignature)
                    {
                        packetSignatureStatus = await CheckPacketSignature(
                            _peerDatabase,
                            _peerIpTarget,
                            _peerUniqueIdTarget,
                            _peerNetworkSettingObject,
                            peerPacketRecvObject.PacketContent,
                            packetResponseExpected,
                            peerPacketRecvObject.PacketHash,
                            peerPacketRecvObject.PacketSignature,
                            _peerCancellationToken);
                    }

                    if (!packetSignatureStatus)
                        return translatedPacket;

                    if (!TryDecryptPacketPeerContent(_peerDatabase, _peerIpTarget, _peerUniqueIdTarget, peerPacketRecvObject.PacketContent, _peerCancellationToken, out byte[] packetDecrypted))
                        return translatedPacket;

                    if (packetDecrypted == null || packetDecrypted.Length == 0)
                        return translatedPacket;

                    if (DeserializePacketContent(packetDecrypted.GetStringFromByteArrayUtf8(), out translatedPacket.PacketTranslated))
                    {
                        // FIX: Equals() was called on a possibly null object.
                        if (translatedPacket.PacketTranslated != null && !translatedPacket.PacketTranslated.Equals(default(T)))
                            translatedPacket.Status = true;
                    }
                }
                catch
                {
                    translatedPacket.Status = false;
                }

                return translatedPacket;
            }

            #endregion

            #region Internal objects.

            /// <summary>
            /// Store the packet data translated after every checks passed.
            /// </summary>
            internal class ClassTranslatePacket<T>
            {
                public T PacketTranslated;
                public bool Status;
            }

            #endregion
        }
    }
}
