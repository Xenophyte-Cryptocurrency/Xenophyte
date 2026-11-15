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
    public class ClassPeerNetworkBroadcastInstanceMemPool
    {
        private ClassPeerDatabase _peerDatabase;

        // Peer IP | [Peer Unique ID | Client broadcast object]
        // Concurrent: accessed by the instance task and by the stop function.
        private ConcurrentDictionary<string, ConcurrentDictionary<string, ClassPeerNetworkClientBroadcastMemPool>> _listPeerNetworkClientBroadcastMemPoolReceiver;
        private ConcurrentDictionary<string, ConcurrentDictionary<string, ClassPeerNetworkClientBroadcastMemPool>> _listPeerNetworkClientBroadcastMemPoolSender;

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

        /// <summary>
        /// Run the network broadcast mempool instance.
        /// </summary>
        public void RunNetworkBroadcastMemPoolInstanceTask()
        {
            _cancellation = new CancellationTokenSource();
            _peerNetworkBroadcastInstanceMemPoolStatus = true;

            TaskManager.TaskManager.InsertTask(new Action(async () =>
            {
                while (_peerNetworkBroadcastInstanceMemPoolStatus && !_cancellation.IsCancellationRequested)
                {
                    try
                    {
                        #region Generate sender/receiver broadcast instances by targetting public peers.

                        if (_peerDatabase.Count > 0)
                        {
                            foreach (string peerIpTarget in _peerDatabase.Keys.ToArray())
                            {
                                if (peerIpTarget.IsNullOrEmpty(false, out _))
                                    continue;

                                ConcurrentDictionary<string, ClassPeerObject> peerListObject = _peerDatabase[peerIpTarget, _cancellation];

                                if (peerListObject == null)
                                    continue;

                                foreach (string peerUniqueIdTarget in peerListObject.Keys.ToArray())
                                {
                                    if (peerUniqueIdTarget.IsNullOrEmpty(false, out _))
                                        continue;

                                    ClassPeerObject peerObject = _peerDatabase[peerIpTarget, peerUniqueIdTarget, _cancellation];

                                    // The peer may have been removed in the meantime.
                                    if (peerObject == null || !peerObject.PeerIsPublic)
                                        continue;

                                    #region Build receiver instance.

                                    ConcurrentDictionary<string, ClassPeerNetworkClientBroadcastMemPool> receiverList = _listPeerNetworkClientBroadcastMemPoolReceiver.GetOrAdd(peerIpTarget, _ => new ConcurrentDictionary<string, ClassPeerNetworkClientBroadcastMemPool>());

                                    if (!receiverList.ContainsKey(peerUniqueIdTarget))
                                    {
                                        ClassPeerNetworkClientBroadcastMemPool receiver = new ClassPeerNetworkClientBroadcastMemPool(
                                            _peerDatabase,
                                            _cancellation,
                                            peerIpTarget,
                                            peerUniqueIdTarget,
                                            false,
                                            _peerNetworkSettingObject,
                                            _peerFirewallSettingObject);

                                        if (receiverList.TryAdd(peerUniqueIdTarget, receiver))
                                        {
                                            bool success = RunPeerNetworkClientBroadcastMemPool(receiver);

                                            if (!success)
                                            {
                                                receiver.StopTaskAndDisconnect();
                                                receiverList.TryRemove(peerUniqueIdTarget, out _);
                                            }

                                            ClassLog.WriteLine("Run Sync MemPool Receive Mode transaction from Peer: " + peerIpTarget + ":" + peerUniqueIdTarget + " | Status: " + (success ? "success" : "failed") + ".", ClassEnumLogLevelType.LOG_LEVEL_MEMPOOL_BROADCAST, ClassEnumLogWriteLevel.LOG_WRITE_LEVEL_MANDATORY_PRIORITY, false, success ? ConsoleColor.Green : ConsoleColor.Red);
                                        }
                                    }

                                    #endregion

                                    // The sender is only built once the receiver exists (it no longer depends on the receiver having been created during this same pass).
                                    if (!receiverList.ContainsKey(peerUniqueIdTarget))
                                        continue;

                                    #region Build sender instance.

                                    ConcurrentDictionary<string, ClassPeerNetworkClientBroadcastMemPool> senderList = _listPeerNetworkClientBroadcastMemPoolSender.GetOrAdd(peerIpTarget, _ => new ConcurrentDictionary<string, ClassPeerNetworkClientBroadcastMemPool>());

                                    if (!senderList.ContainsKey(peerUniqueIdTarget))
                                    {
                                        ClassPeerNetworkClientBroadcastMemPool sender = new ClassPeerNetworkClientBroadcastMemPool(
                                            _peerDatabase,
                                            _cancellation,
                                            peerIpTarget,
                                            peerUniqueIdTarget,
                                            true,
                                            _peerNetworkSettingObject,
                                            _peerFirewallSettingObject);

                                        if (senderList.TryAdd(peerUniqueIdTarget, sender))
                                        {
                                            bool successRunClientBroadcast = RunPeerNetworkClientBroadcastMemPool(sender);

                                            if (!successRunClientBroadcast)
                                            {
                                                sender.StopTaskAndDisconnect();
                                                senderList.TryRemove(peerUniqueIdTarget, out _);
                                            }

                                            ClassLog.WriteLine("Run Sync Sending Mode MemPool transaction from Peer: " + peerIpTarget + " | Status: " + (successRunClientBroadcast ? "success" : "failed") + ".", ClassEnumLogLevelType.LOG_LEVEL_MEMPOOL_BROADCAST, ClassEnumLogWriteLevel.LOG_WRITE_LEVEL_MANDATORY_PRIORITY, false, successRunClientBroadcast ? ConsoleColor.Green : ConsoleColor.Red);
                                        }
                                    }

                                    #endregion
                                }
                            }
                        }

                        #endregion

                        #region Check Client MemPool broadcast launched.

                        // Check receiver mode instances.
                        CleanDeadInstances(_listPeerNetworkClientBroadcastMemPoolReceiver, "receiver");

                        // Check sender mode instances (also cleaned when the node is not public, because senders are built in both cases).
                        CleanDeadInstances(_listPeerNetworkClientBroadcastMemPoolSender, "sender");

                        #endregion
                    }
                    catch (Exception error)
                    {
                        ClassLog.WriteLine("Error on generating MemPool instance. Exception: " + error.Message, ClassEnumLogLevelType.LOG_LEVEL_MEMPOOL_BROADCAST, ClassEnumLogWriteLevel.LOG_WRITE_LEVEL_MANDATORY_PRIORITY, false, ConsoleColor.Red);
                    }

                    await Task.Delay(1000);
                }

            }), 0, _cancellation, null).Wait();
        }

        /// <summary>
        /// Remove dead client broadcast instances (receiver or sender).
        /// </summary>
        private void CleanDeadInstances(ConcurrentDictionary<string, ConcurrentDictionary<string, ClassPeerNetworkClientBroadcastMemPool>> instanceList, string mode)
        {
            foreach (string peerIp in instanceList.Keys.ToArray())
            {
                try
                {
                    if (!instanceList.TryGetValue(peerIp, out ConcurrentDictionary<string, ClassPeerNetworkClientBroadcastMemPool> peerInstances))
                        continue;

                    foreach (string peerUniqueId in peerInstances.Keys.ToArray())
                    {
                        try
                        {
                            if (peerInstances.TryGetValue(peerUniqueId, out ClassPeerNetworkClientBroadcastMemPool instance) && !instance.IsAlive)
                            {
                                instance.StopTaskAndDisconnect();
                                peerInstances.TryRemove(peerUniqueId, out _);

#if DEBUG
                                Debug.WriteLine("MemPool Broadcast instance " + mode + " of peer " + peerIp + ":" + peerUniqueId + " is dead.");
#endif
                            }
                        }
                        catch
                        {
                            // Ignored: continue with the next instance.
                        }
                    }

                    if (peerInstances.IsEmpty)
                        instanceList.TryRemove(peerIp, out _);
                }
                catch
                {
                    // Ignored: continue with the next peer.
                }
            }
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
            catch
            {
                return false;
            }

            return true;
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

            #region Clean client broadcast instances.

            StopAndClear(_listPeerNetworkClientBroadcastMemPoolReceiver);
            StopAndClear(_listPeerNetworkClientBroadcastMemPoolSender);

            #endregion
        }

        /// <summary>
        /// Stop every instance of a list and clear it.
        /// </summary>
        private void StopAndClear(ConcurrentDictionary<string, ConcurrentDictionary<string, ClassPeerNetworkClientBroadcastMemPool>> instanceList)
        {
            foreach (string peerIp in instanceList.Keys.ToArray())
            {
                try
                {
                    if (!instanceList.TryGetValue(peerIp, out ConcurrentDictionary<string, ClassPeerNetworkClientBroadcastMemPool> peerInstances))
                        continue;

                    foreach (string peerUniqueId in peerInstances.Keys.ToArray())
                    {
                        try
                        {
                            if (peerInstances.TryGetValue(peerUniqueId, out ClassPeerNetworkClientBroadcastMemPool instance))
                                instance.StopTaskAndDisconnect();
                        }
                        catch
                        {
                            // Ignored.
                        }
                    }

                    peerInstances.Clear();
                }
                catch
                {
                    // Ignored.
                }
            }

            instanceList.Clear();
        }

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

            // Block height | [Transaction hash | unused]. Concurrent: used by the task and by StopTaskAndDisconnect.
            private ConcurrentDictionary<long, ConcurrentDictionary<string, byte>> _memPoolListBlockHeightTransactionReceived;
            private ConcurrentDictionary<long, ConcurrentDictionary<string, byte>> _memPoolListBlockHeightTransactionSend;
            private bool _onSendBroadcastMode;

            /// <summary>
            /// Transaction hashes already known by the mempool, shared by every client instance (every peer).
            /// Prevents inserting the same transaction several times when it is received from multiple peers.
            /// Transaction hash | Block height target.
            /// </summary>
            private static readonly ConcurrentDictionary<string, long> KnownMemPoolTransactionHashes = new ConcurrentDictionary<string, long>();
            private static long _lastKnownHashesCleanedHeight = -1;

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
            private int _stopped;

            ~ClassPeerNetworkClientBroadcastMemPool()
            {
                Dispose(false);
            }

            public void Dispose()
            {
                Dispose(true);
                GC.SuppressFinalize(this);
            }

            // Protected implementation of Dispose pattern.
            protected virtual void Dispose(bool disposing)
            {
                if (_disposed)
                    return;

                _disposed = true;

                if (disposing)
                    StopTaskAndDisconnect();
            }

            #endregion

            #region Initialize/Stop broadcast client.

            /// <summary>
            /// Stop tasks and disconnect.
            /// </summary>
            public void StopTaskAndDisconnect()
            {
                IsAlive = false;

                // Stop and release the token only once.
                if (Interlocked.Exchange(ref _stopped, 1) == 0)
                {
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
                            // Always release the linked token, even if the parent token was already cancelled.
                            _peerCancellationToken.Dispose();
                        }
                        catch
                        {
                            // Ignored.
                        }
                    }
                }

                // Clean up past heights received/sent.
                _memPoolListBlockHeightTransactionReceived?.Clear();
                _memPoolListBlockHeightTransactionSend?.Clear();
            }

            #endregion

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
            /// Remove the shared known transaction hashes of the blocks already unlocked (done once per height, not once per peer).
            /// </summary>
            private static void CleanKnownTransactionHashes(long lastBlockHeightUnlocked)
            {
                if (Interlocked.Exchange(ref _lastKnownHashesCleanedHeight, lastBlockHeightUnlocked) == lastBlockHeightUnlocked)
                    return;

                foreach (KeyValuePair<string, long> knownHash in KnownMemPoolTransactionHashes.ToArray())
                {
                    if (knownHash.Value <= lastBlockHeightUnlocked)
                        KnownMemPoolTransactionHashes.TryRemove(knownHash.Key, out _);
                }
            }

            /// <summary>
            /// Enable the received broadcast transaction task.
            /// </summary>
            private void EnableReceiveBroadcastTransactionTask()
            {
                TaskManager.TaskManager.InsertTask(new Action(async () =>
                {
                    using (DisposableDictionary<string, string> listWalletAddressAndPublicKeyCache = new DisposableDictionary<string, string>())
                    {
                        while (IsAlive && !_peerCancellationToken.IsCancellationRequested)
                        {
                            try
                            {
                                ClassPeerObject peerObject = _peerDatabase[_peerIpTarget, _peerUniqueIdTarget, _peerCancellationToken];

                                if (peerObject == null)
                                {
                                    IsAlive = false;
                                    break;
                                }

                                long lastBlockHeightUnlocked = await ClassBlockchainDatabase.BlockchainMemoryManagement.GetLastBlockHeightUnlocked(_peerCancellationToken);

                                // Clean passed blocks mined.
                                foreach (long blockHeight in _memPoolListBlockHeightTransactionReceived.Keys.ToArray())
                                {
                                    if (blockHeight <= lastBlockHeightUnlocked)
                                        _memPoolListBlockHeightTransactionReceived.TryRemove(blockHeight, out _);
                                }

                                CleanKnownTransactionHashes(lastBlockHeightUnlocked);

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
                                {
                                    IsAlive = false;
                                    break;
                                }

                                ClassPeerNetworkClientSyncObject peerNetworkClientSyncObject = new ClassPeerNetworkClientSyncObject(_peerDatabase, _peerIpTarget, peerObject.PeerPort, _peerUniqueIdTarget, _peerNetworkSettingObject, _peerFirewallSettingObject);

                                ClassLog.WriteLine("Try to send ask MemPool block height list: " + _peerIpTarget + " request.", ClassEnumLogLevelType.LOG_LEVEL_MEMPOOL_BROADCAST, ClassEnumLogWriteLevel.LOG_WRITE_LEVEL_MANDATORY_PRIORITY, false, ConsoleColor.Yellow);

                                if (!await peerNetworkClientSyncObject.TrySendPacketToPeerTarget(packetSendObject, true, peerObject.PeerPort, _peerUniqueIdTarget, _peerCancellationToken, ClassPeerEnumPacketResponse.SEND_MEM_POOL_BLOCK_HEIGHT_LIST_BROADCAST_MODE, false, false))
                                {
                                    ClassLog.WriteLine("Try to send ask MemPool block height list: " + _peerIpTarget + " failed.", ClassEnumLogLevelType.LOG_LEVEL_MEMPOOL_BROADCAST, ClassEnumLogWriteLevel.LOG_WRITE_LEVEL_MANDATORY_PRIORITY, false, ConsoleColor.Yellow);
                                    IsAlive = false;
                                    break;
                                }

                                if (peerNetworkClientSyncObject.PeerPacketReceived == null)
                                {
                                    IsAlive = false;
                                    break;
                                }

                                ClassTranslatePacket<ClassPeerPacketSendMemPoolBlockHeightList> peerPacketMemPoolBlockHeightListTranslated = await TranslatePacketReceived<ClassPeerPacketSendMemPoolBlockHeightList>(peerNetworkClientSyncObject.PeerPacketReceived, ClassPeerEnumPacketResponse.SEND_MEM_POOL_BLOCK_HEIGHT_LIST_BROADCAST_MODE, _peerCancellationToken);

                                // Check the status first: PacketTranslated is only valid when the translation succeeded.
                                if (peerPacketMemPoolBlockHeightListTranslated == null ||
                                    !peerPacketMemPoolBlockHeightListTranslated.Status ||
                                    peerPacketMemPoolBlockHeightListTranslated.PacketTranslated == null)
                                {
                                    IsAlive = false;
                                    break;
                                }

                                #endregion

                                #region Check the packet of mempool block height lists.

                                if (peerPacketMemPoolBlockHeightListTranslated.PacketTranslated.MemPoolBlockHeightListAndCount == null)
                                {
                                    IsAlive = false;
                                    break;
                                }

                                ClassLog.WriteLine("Request to ask MemPool block height list: " + _peerIpTarget + " done. Result: " + JsonConvert.SerializeObject(peerPacketMemPoolBlockHeightListTranslated.PacketTranslated.MemPoolBlockHeightListAndCount), ClassEnumLogLevelType.LOG_LEVEL_MEMPOOL_BROADCAST, ClassEnumLogWriteLevel.LOG_WRITE_LEVEL_MANDATORY_PRIORITY, false, ConsoleColor.Yellow);

                                #endregion

                                #region Then sync transaction by height.

                                if (peerPacketMemPoolBlockHeightListTranslated.PacketTranslated.MemPoolBlockHeightListAndCount.Count > 0)
                                {
                                    bool failed = false;

                                    foreach (long blockHeight in peerPacketMemPoolBlockHeightListTranslated.PacketTranslated.MemPoolBlockHeightListAndCount.Keys.OrderBy(x => x))
                                    {
                                        if (_peerCancellationToken.IsCancellationRequested || failed)
                                            break;

                                        if (ClassBlockchainDatabase.BlockchainMemoryManagement.GetLastBlockHeight < blockHeight)
                                        {
                                            ConcurrentDictionary<string, byte> receivedHashes = _memPoolListBlockHeightTransactionReceived.GetOrAdd(blockHeight, _ => new ConcurrentDictionary<string, byte>());

                                            int countAlreadySynced = receivedHashes.Count;

                                            if (peerPacketMemPoolBlockHeightListTranslated.PacketTranslated.MemPoolBlockHeightListAndCount[blockHeight] > 0)
                                            {
                                                long lastBlockHeightUnlockedConfirmed = await ClassBlockchainDatabase.BlockchainMemoryManagement.GetLastBlockHeightTransactionConfirmationDone(_peerCancellationToken);

                                                if (lastBlockHeightUnlockedConfirmed == blockHeight)
                                                    continue;

                                                int countToSync = peerPacketMemPoolBlockHeightListTranslated.PacketTranslated.MemPoolBlockHeightListAndCount[blockHeight];

                                                if (countToSync != countAlreadySynced)
                                                {
                                                    // Ensure to be compatible with most recent transactions sent.
                                                    packetSendObject = new ClassPeerPacketSendObject(_peerNetworkSettingObject.PeerUniqueId,
                                                        peerObject.PeerInternPublicKey,
                                                        peerObject.PeerClientLastTimestampPeerPacketSignatureWhitelist)
                                                    {
                                                        PacketOrder = ClassPeerEnumPacketSend.ASK_MEM_POOL_TRANSACTION_BY_BLOCK_HEIGHT_BROADCAST_MODE,
                                                        PacketContent = ClassUtility.SerializeData(new ClassPeerPacketSendAskMemPoolTransactionList()
                                                        {
                                                            BlockHeight = blockHeight,
                                                            TotalTransactionProgress = 0,
                                                            PacketTimestamp = TaskManager.TaskManager.CurrentTimestampSecond
                                                        }),
                                                    };

                                                    packetSendObject = await ClassPeerNetworkBroadcastShortcutFunction.BuildSignedPeerSendPacketObject(_peerDatabase, packetSendObject, _peerIpTarget, _peerUniqueIdTarget, false, _peerNetworkSettingObject, _peerCancellationToken);

                                                    if (packetSendObject == null)
                                                    {
                                                        IsAlive = false;
                                                        failed = true;
                                                        break;
                                                    }

                                                    if (!await peerNetworkClientSyncObject.TrySendPacketToPeerTarget(packetSendObject, true, peerObject.PeerPort, _peerUniqueIdTarget, _peerCancellationToken, ClassPeerEnumPacketResponse.SEND_MEM_POOL_END_TRANSACTION_BY_BLOCK_HEIGHT_BROADCAST_MODE, false, false))
                                                    {
                                                        IsAlive = false;
                                                        failed = true;
                                                        break;
                                                    }

                                                    ClassTranslatePacket<ClassPeerPacketSendMemPoolTransaction> packetTranslated = await TranslatePacketReceived<ClassPeerPacketSendMemPoolTransaction>(peerNetworkClientSyncObject.PeerPacketReceived, ClassPeerEnumPacketResponse.SEND_MEM_POOL_TRANSACTION_BY_BLOCK_HEIGHT_BROADCAST_MODE, _peerCancellationToken);

                                                    if (packetTranslated.Status &&
                                                        packetTranslated.PacketTranslated != null &&
                                                        packetTranslated.PacketTranslated.ListTransactionObject != null &&
                                                        packetTranslated.PacketTranslated.ListTransactionObject.Count > 0)
                                                    {
                                                        foreach (ClassTransactionObject transactionObject in packetTranslated.PacketTranslated.ListTransactionObject)
                                                        {
                                                            if (transactionObject == null)
                                                                continue;

                                                            ClassLog.WriteLine("Transaction object received from peer: " + _peerIpTarget + " | Data: " + JsonConvert.SerializeObject(transactionObject), ClassEnumLogLevelType.LOG_LEVEL_MEMPOOL_BROADCAST, ClassEnumLogWriteLevel.LOG_WRITE_LEVEL_MANDATORY_PRIORITY, false, ConsoleColor.Yellow);

                                                            if (transactionObject.TransactionType == ClassTransactionEnumType.BLOCK_REWARD_TRANSACTION ||
                                                                transactionObject.TransactionType == ClassTransactionEnumType.DEV_FEE_TRANSACTION)
                                                                continue;

                                                            string transactionHash = transactionObject.TransactionHash;

                                                            if (string.IsNullOrEmpty(transactionHash))
                                                                continue;

                                                            // Already received from this peer.
                                                            if (receivedHashes.ContainsKey(transactionHash))
                                                                continue;

                                                            // Claim the hash for every peer: if another peer instance already handled this transaction, do not insert it twice.
                                                            if (!KnownMemPoolTransactionHashes.TryAdd(transactionHash, blockHeight))
                                                            {
                                                                receivedHashes.TryAdd(transactionHash, 0);
                                                                continue;
                                                            }

                                                            bool keepClaim = false;

                                                            try
                                                            {
                                                                if (transactionObject.BlockHeightTransaction > ClassBlockchainDatabase.BlockchainMemoryManagement.GetLastBlockHeight)
                                                                {
                                                                    ClassTransactionEnumStatus checkTxResult = await ClassTransactionUtility.CheckTransactionWithBlockchainData(transactionObject, true, false, true, null, 0, listWalletAddressAndPublicKeyCache, true, _peerCancellationToken);

                                                                    if (checkTxResult == ClassTransactionEnumStatus.VALID_TRANSACTION)
                                                                    {
                                                                        ClassMemPoolDatabase.InsertTxToMemPool(transactionObject);
                                                                        receivedHashes.TryAdd(transactionHash, 0);
                                                                        keepClaim = true;
                                                                    }
                                                                    else if (checkTxResult == ClassTransactionEnumStatus.DUPLICATE_TRANSACTION_HASH)
                                                                    {
                                                                        // Already known by the node: do not insert it again, only remember it.
                                                                        receivedHashes.TryAdd(transactionHash, 0);
                                                                        keepClaim = true;
                                                                    }
                                                                }
                                                            }
                                                            finally
                                                            {
                                                                // Invalid transaction (or error): release the claim so it can be checked again later.
                                                                if (!keepClaim)
                                                                    KnownMemPoolTransactionHashes.TryRemove(transactionHash, out _);
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }

                                #endregion
                            }
                            catch (Exception error)
                            {
                                ClassLog.WriteLine("Task to receive MemPool transaction failed from: " + _peerIpTarget + " | Exception. Result: " + error.Message, ClassEnumLogLevelType.LOG_LEVEL_MEMPOOL_BROADCAST, ClassEnumLogWriteLevel.LOG_WRITE_LEVEL_MANDATORY_PRIORITY, false, ConsoleColor.Yellow);
                            }

                            await Task.Delay(_peerNetworkSettingObject.PeerTaskSyncDelay);
                        }
                    }

                }), 0, _peerCancellationToken).Wait();

#if DEBUG
                Debug.WriteLine("Enable Receive Broadcast transaction MemPool from: " + _peerIpTarget + " done. " + IsAlive);
#endif
            }

            /// <summary>
            /// Enable the send broadcast transaction task.
            /// </summary>
            private void EnableSendBroadcastTransactionTask()
            {
                TaskManager.TaskManager.InsertTask(new Action(async () =>
                {
                    while (IsAlive && !_peerCancellationToken.IsCancellationRequested)
                    {
                        try
                        {
                            ClassPeerObject peerObject = _peerDatabase[_peerIpTarget, _peerUniqueIdTarget, _peerCancellationToken];

                            if (peerObject == null)
                            {
                                IsAlive = false;
                                break;
                            }

                            // Clean the sent hashes of the blocks already unlocked.
                            long lastBlockHeightUnlocked = await ClassBlockchainDatabase.BlockchainMemoryManagement.GetLastBlockHeightUnlocked(_peerCancellationToken);

                            foreach (long blockHeightSent in _memPoolListBlockHeightTransactionSend.Keys.ToArray())
                            {
                                if (blockHeightSent <= lastBlockHeightUnlocked)
                                    _memPoolListBlockHeightTransactionSend.TryRemove(blockHeightSent, out _);
                            }

                            using (DisposableList<long> listMemPoolBlockHeight = await ClassMemPoolDatabase.GetMemPoolListBlockHeight(_peerCancellationToken))
                            {
                                if (listMemPoolBlockHeight.Count > 0)
                                {
                                    foreach (long blockHeight in listMemPoolBlockHeight.GetList)
                                    {
                                        if (_peerCancellationToken.IsCancellationRequested)
                                            break;

                                        int countMemPoolTransaction = await ClassMemPoolDatabase.GetCountMemPoolTxFromBlockHeight(blockHeight, true, _peerCancellationToken);

                                        if (countMemPoolTransaction <= 0)
                                            continue;

                                        ConcurrentDictionary<string, byte> sentHashes = _memPoolListBlockHeightTransactionSend.GetOrAdd(blockHeight, _ => new ConcurrentDictionary<string, byte>());

                                        using (DisposableList<ClassTransactionObject> listTransactionObjectInMemPool = await ClassMemPoolDatabase.GetMemPoolTxObjectFromBlockHeight(blockHeight, true, _peerCancellationToken))
                                        {
                                            // Only send the transactions not already sent to this peer, without duplicates inside the packet.
                                            List<ClassTransactionObject> listTransactionObjectToSend = listTransactionObjectInMemPool.GetList
                                                .Where(tx => tx != null && !string.IsNullOrEmpty(tx.TransactionHash) && !sentHashes.ContainsKey(tx.TransactionHash))
                                                .GroupBy(tx => tx.TransactionHash)
                                                .Select(group => group.First())
                                                .ToList();

                                            if (listTransactionObjectToSend.Count == 0)
                                                continue;

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

                                            if (packetSendObject == null)
                                            {
                                                IsAlive = false;
                                                break;
                                            }

                                            ClassPeerNetworkClientSyncObject peerNetworkClientSyncObject = new ClassPeerNetworkClientSyncObject(_peerDatabase, _peerIpTarget, peerObject.PeerPort, _peerUniqueIdTarget, _peerNetworkSettingObject, _peerFirewallSettingObject);

                                            if (!await peerNetworkClientSyncObject.TrySendPacketToPeerTarget(packetSendObject, true, peerObject.PeerPort, _peerUniqueIdTarget, _peerCancellationToken, ClassPeerEnumPacketResponse.SEND_MEM_POOL_TRANSACTION_VOTE, true, false))
                                            {
                                                IsAlive = false;
                                                break;
                                            }

                                            ClassTranslatePacket<ClassPeerPacketSendMemPoolTransactionVote> packetTranslated = await TranslatePacketReceived<ClassPeerPacketSendMemPoolTransactionVote>(peerNetworkClientSyncObject.PeerPacketReceived, ClassPeerEnumPacketResponse.SEND_MEM_POOL_TRANSACTION_VOTE, _peerCancellationToken);

                                            if (packetTranslated.Status)
                                            {
                                                foreach (ClassTransactionObject transactionObject in listTransactionObjectToSend)
                                                    sentHashes.TryAdd(transactionObject.TransactionHash, 0);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        catch (OperationCanceledException)
                        {
                            IsAlive = false;
                            break;
                        }
                        catch (Exception error)
                        {
                            // A transient error no longer kills the sender: it is retried at the next cycle.
                            ClassLog.WriteLine("Task to send MemPool transaction failed to: " + _peerIpTarget + " | Exception. Result: " + error.Message, ClassEnumLogLevelType.LOG_LEVEL_MEMPOOL_BROADCAST, ClassEnumLogWriteLevel.LOG_WRITE_LEVEL_MANDATORY_PRIORITY, false, ConsoleColor.Yellow);
                        }

                        await Task.Delay(_peerNetworkSettingObject.PeerTaskSyncDelay);
                    }

                }), 0, _peerCancellationToken).Wait();

#if DEBUG
                Debug.WriteLine("Enable Send Broadcast transaction MemPool from: " + _peerIpTarget + " done.");
#endif
            }

            #endregion

            #region Packet management.

            /// <summary>
            /// Translate the packet received.
            /// </summary>
            /// <typeparam name="T"></typeparam>
            /// <param name="peerPacketRecvObject"></param>
            /// <param name="packetResponseExpected"></param>
            /// <returns></returns>
            private async Task<ClassTranslatePacket<T>> TranslatePacketReceived<T>(ClassPeerPacketRecvObject peerPacketRecvObject, ClassPeerEnumPacketResponse packetResponseExpected, CancellationTokenSource cancellation)
            {
                ClassTranslatePacket<T> translatedPacket = new ClassTranslatePacket<T>();

                if (peerPacketRecvObject != null)
                {
                    if (!peerPacketRecvObject.PacketContent.IsNullOrEmpty(false, out _))
                    {
                        try
                        {
                            bool peerIgnorePacketSignature = ClassPeerCheckManager.CheckPeerClientWhitelistStatus(_peerDatabase, _peerIpTarget, _peerUniqueIdTarget, _peerNetworkSettingObject, cancellation);

                            if (!peerIgnorePacketSignature)
                            {
                                ClassPeerObject peerObject = _peerDatabase[_peerIpTarget, _peerUniqueIdTarget, _peerCancellationToken];

                                if (peerObject != null)
                                    peerIgnorePacketSignature = peerObject.PeerTimestampSignatureWhitelist >= TaskManager.TaskManager.CurrentTimestampSecond;
                            }

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

                            if (packetSignatureStatus)
                            {
                                if (TryDecryptPacketPeerContent(_peerDatabase, _peerIpTarget, _peerUniqueIdTarget, peerPacketRecvObject.PacketContent, _peerCancellationToken, out byte[] packetDecrypted))
                                {
                                    if (packetDecrypted != null && packetDecrypted.Length > 0)
                                    {
                                        if (DeserializePacketContent(packetDecrypted.GetStringFromByteArrayUtf8(), out translatedPacket.PacketTranslated))
                                        {
                                            // Null-safe comparison: PacketTranslated may be null for reference types.
                                            if (!EqualityComparer<T>.Default.Equals(translatedPacket.PacketTranslated, default(T)))
                                                translatedPacket.Status = true;
                                        }
                                    }
                                }
                            }
                        }
                        catch
                        {
                            translatedPacket.Status = false;
                        }
                    }
                }

                return translatedPacket;
            }

            #endregion

            #region Internal objects.

            /// <summary>
            /// Store the packet data translated after every checks passed.
            /// </summary>
            /// <typeparam name="T"></typeparam>
            internal class ClassTranslatePacket<T>
            {
                public T PacketTranslated;
                public bool Status;
            }

            #endregion
        }
    }
}
