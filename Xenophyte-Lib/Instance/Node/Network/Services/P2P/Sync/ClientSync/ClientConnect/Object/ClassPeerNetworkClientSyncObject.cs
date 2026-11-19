using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Xenophyte_Lib.Instance.Node.Network.Database;
using Xenophyte_Lib.Instance.Node.Network.Database.Manager;
using Xenophyte_Lib.Instance.Node.Network.Enum.P2P.Packet;
using Xenophyte_Lib.Instance.Node.Network.Services.P2P.Broadcast;
using Xenophyte_Lib.Instance.Node.Network.Services.P2P.Sync.Packet;
using Xenophyte_Lib.Instance.Node.Network.Services.P2P.Sync.Packet.Model;
using Xenophyte_Lib.Instance.Node.Network.Services.P2P.Sync.Packet.SubPacket.Request;
using Xenophyte_Lib.Instance.Node.Setting.Object;
using Xenophyte_Lib.Log;
using Xenophyte_Lib.Other.Object.List;
using Xenophyte_Lib.Other.Object.Network;
using Xenophyte_Lib.Utility;
using static Xenophyte_Lib.Other.Object.Network.ClassCustomSocket;

namespace Xenophyte_Lib.Instance.Node.Network.Services.P2P.Sync.ClientSync.ClientConnect.Object
{
    public class ClassPeerNetworkClientSyncObject : IDisposable
    {
        /// <summary>
        /// Tcp info and tcp client object.
        /// </summary>
        private ClassCustomSocket _peerSocketClient;
        public bool PeerConnectStatus => _peerSocketClient != null ? _peerSocketClient.IsConnected() : false;

        /// <summary>
        /// Peer informations.
        /// </summary>
        public string PeerIpTarget;
        public int PeerPortTarget;
        public int PeerApiPortTarget;
        public string PeerUniqueIdTarget;
     

        /// <summary>
        /// Packet received.
        /// </summary>
        public ClassPeerPacketRecvObject PeerPacketReceived;
        public ClassPeerEnumPacketResponse PeerPacketTypeReceived;
        public bool PeerPacketReceivedStatus;

        /// <summary>
        /// Peer task status.
        /// </summary>
        public bool PeerTaskStatus;
        private bool _peerTaskKeepAliveStatus;
        private CancellationTokenSource _peerCancellationTokenMain;
        private CancellationTokenSource _peerCancellationTokenKeepAlive;
        // The legacy packet protocol is a byte stream: concurrent writes can
        // interleave packet fragments and corrupt framing.
        private readonly SemaphoreSlim _peerSendLock = new SemaphoreSlim(1, 1);
        private readonly object _peerResponseLock = new object();
        private Task _peerReceiveTask;
        private TaskCompletionSource<bool> _peerPendingResponse;
        private int _binaryPacketId;

        /// <summary>
        /// Peer Database.
        /// </summary>
        private ClassPeerDatabase _peerDatabase;

        /// <summary>
        /// Network settings.
        /// </summary>
        private ClassPeerNetworkSettingObject _peerNetworkSetting;
        private ClassPeerFirewallSettingObject _peerFirewallSettingObject;

        /// <summary>
        /// Specifications of the connection opened.
        /// </summary>
        public ClassPeerEnumPacketResponse PacketResponseExpected;
        private bool _keepAlive;


        #region Dispose functions

        ~ClassPeerNetworkClientSyncObject()
        {
            Dispose(true);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        // Protected implementation of Dispose pattern.
        protected virtual void Dispose(bool disposing)
        {
            PeerTaskStatus = false;
            CleanUpTask();
            DisconnectFromTarget();
        }

        #endregion

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="peerIpTarget"></param>
        /// <param name="peerPort"></param>
        /// <param name="peerUniqueId"></param>
        /// <param name="peerNetworkSetting"></param>
        /// <param name="peerFirewallSettingObject"></param>
        public ClassPeerNetworkClientSyncObject(ClassPeerDatabase peerDatabase, string peerIpTarget, int peerPort, string peerUniqueId, ClassPeerNetworkSettingObject peerNetworkSetting, ClassPeerFirewallSettingObject peerFirewallSettingObject)
        {
            _peerDatabase = peerDatabase;
            PeerIpTarget = peerIpTarget;
            PeerPortTarget = peerPort;
            PeerUniqueIdTarget = peerUniqueId;
            _peerNetworkSetting = peerNetworkSetting;
            _peerFirewallSettingObject = peerFirewallSettingObject;
        }

        /// <summary>
        /// Attempt to send a packet to a peer target.
        /// </summary>
        /// <param name="packetSendObject"></param>
        /// <param name="cancellation"></param>
        /// <param name="packetResponseExpected"></param>
        /// <param name="keepAlive"></param>
        /// <param name="broadcast"></param>
        /// <returns></returns>
        public async Task<bool> TrySendPacketToPeerTarget(ClassPeerPacketSendObject packetSendObject, bool toSignAndEncrypt, int peerPort, string peerUniqueId, CancellationTokenSource cancellation, ClassPeerEnumPacketResponse packetResponseExpected, bool keepAlive, bool broadcast)
        {

            #region Clean up and cancel previous task.

            CleanUpTask();

            #endregion

            _peerCancellationTokenMain = CancellationTokenSource.CreateLinkedTokenSource(cancellation.Token);

            if (toSignAndEncrypt)
            {
                packetSendObject = await ClassPeerNetworkBroadcastShortcutFunction.BuildSignedPeerSendPacketObject(_peerDatabase, packetSendObject, PeerIpTarget, peerUniqueId, false, _peerNetworkSetting, _peerCancellationTokenMain);

                if (packetSendObject == null ||
                    packetSendObject.PacketContent.IsNullOrEmpty(false, out _) ||
                    packetSendObject.PacketHash.IsNullOrEmpty(false, out _) ||
                    packetSendObject.PacketSignature.IsNullOrEmpty(false, out _) ||
                    packetSendObject.PublicKey.IsNullOrEmpty(false, out _))
                {
#if DEBUG
                    Debug.WriteLine("Failed, to sign packet data target " + PeerIpTarget + " | Type: " + System.Enum.GetName(typeof(ClassPeerEnumPacketSend), PacketResponseExpected));
#endif
                    return false;
                }
            }


            byte[] packetData = packetSendObject.GetPacketData();

            if (packetData == null)
            {
#if DEBUG
                Debug.WriteLine("Failed, packet data empty target" + PeerIpTarget + " | Type: " + System.Enum.GetName(typeof(ClassPeerEnumPacketSend), packetSendObject.PacketOrder));
#endif
                return false;
            }
            #region Init the client sync object.

            PacketResponseExpected = packetResponseExpected;
            _keepAlive = keepAlive;
            PeerPortTarget = peerPort;
            PeerUniqueIdTarget = peerUniqueId;

            #endregion

            #region Check the current connection status opened to the target.

            if (!PeerConnectStatus || !keepAlive)
            {
                DisconnectFromTarget();


                if (!await DoConnection(_peerCancellationTokenMain))
                {
#if DEBUG
                    Debug.WriteLine("Failed to connect to peer " + PeerIpTarget + ":" + PeerPortTarget);
#endif
                    ClassLog.WriteLine("Failed to connect to peer " + PeerIpTarget + ":" + PeerPortTarget, ClassEnumLogLevelType.LOG_LEVEL_PEER_TASK_SYNC, ClassEnumLogWriteLevel.LOG_WRITE_LEVEL_LOWEST_PRIORITY, true);
                    ClassPeerCheckManager.InputPeerClientAttemptConnect(_peerDatabase, PeerIpTarget, PeerUniqueIdTarget, _peerNetworkSetting, _peerFirewallSettingObject, _peerCancellationTokenMain);
                    DisconnectFromTarget();
                    return false;
                }
            }

            #endregion

            // Keep exactly one reader on the TCP stream for the lifetime of this
            // client connection. Responses to requests and keep-alive traffic are
            // demultiplexed by that reader instead of competing for NetworkStream.
            await EnsurePeerReceiveLoop().ConfigureAwait(false);
            if (_peerReceiveTask == null || !PeerConnectStatus)
                return false;

            TaskCompletionSource<bool> pendingResponse = null;
            if (!broadcast)
            {
                pendingResponse = new TaskCompletionSource<bool>(
                    TaskCreationOptions.RunContinuationsAsynchronously);
                lock (_peerResponseLock)
                    _peerPendingResponse = pendingResponse;
            }

            #region Send packet and wait packet response.

            if (!await SendPeerPacket(packetData, _peerCancellationTokenMain))
            {
#if DEBUG
                Debug.WriteLine("Failed to send packet data to " + PeerIpTarget + ":" + PeerPortTarget);
#endif
                ClassPeerCheckManager.InputPeerClientNoPacketConnectionOpened(_peerDatabase, PeerIpTarget, PeerUniqueIdTarget, _peerNetworkSetting, _peerFirewallSettingObject, _peerCancellationTokenMain);
                lock (_peerResponseLock)
                {
                    if (ReferenceEquals(_peerPendingResponse, pendingResponse))
                        _peerPendingResponse = null;
                }
                DisconnectFromTarget();
                return false;
            }
            else
                return broadcast ? true : await WaitPacketExpected(pendingResponse);

            #endregion

        }

        #region Initialize connection functions.

        /// <summary>
        /// Clean up the task.
        /// </summary>
        private void CleanUpTask()
        {
            PeerPacketTypeReceived = ClassPeerEnumPacketResponse.NONE;
            PeerPacketReceived = null;
            PeerPacketReceivedStatus = false;
            lock (_peerResponseLock)
                _peerPendingResponse = null;
            CancelKeepAlive();
            try
            {
                if (_peerCancellationTokenMain != null)
                {
                    if (!_peerCancellationTokenMain.IsCancellationRequested)
                        _peerCancellationTokenMain.Cancel();
                }
            }
            catch
            {
                // Ignored, if already cancelled.
            }
        }


        /// <summary>
        /// Do connection.
        /// </summary>
        /// <returns></returns>
        private async Task<bool> DoConnection(CancellationTokenSource cancellation)
        {
            _peerSocketClient = new ClassCustomSocket(new Socket(ClassUtility.GetAddressFamily(PeerIpTarget), SocketType.Stream, ProtocolType.Tcp), false);

            if (await _peerSocketClient.Connect(PeerIpTarget, PeerPortTarget, _peerNetworkSetting.PeerMaxDelayToConnectToTarget, cancellation))
                return true;
            else _peerSocketClient?.Kill(SocketShutdown.Both);

            await Task.Delay(10);

            return false;
        }


        /// <summary>
        /// Wait the packet expected.
        /// </summary>
        /// <returns></returns>
        private async Task<bool> WaitPacketExpected(TaskCompletionSource<bool> pendingResponse)
        {
            if (pendingResponse == null)
                return false;

            int timeoutMilliseconds = Math.Max(1, _peerNetworkSetting.PeerApiMaxConnectionDelay) * 1000;
            Task completed = await Task.WhenAny(
                pendingResponse.Task,
                Task.Delay(timeoutMilliseconds, _peerCancellationTokenMain.Token));

            if (completed != pendingResponse.Task ||
                _peerCancellationTokenMain.IsCancellationRequested)
            {
                lock (_peerResponseLock)
                {
                    if (ReferenceEquals(_peerPendingResponse, pendingResponse))
                        _peerPendingResponse = null;
                }

#if DEBUG
                Debug.WriteLine("Peer " + PeerIpTarget + " timed out waiting for " +
                    System.Enum.GetName(typeof(ClassPeerEnumPacketResponse), PacketResponseExpected));
#endif
                if (!_keepAlive)
                    DisconnectFromTarget();
                return false;
            }

            if (!PeerPacketReceivedStatus || PeerPacketReceived == null)
                return false;

            if (!_keepAlive)
                DisconnectFromTarget();
            else
                TaskEnablePeerPacketKeepAlive();

            return true;
        }

        #endregion

        #region Wait packet to receive functions.

        /// <summary>
        /// Task in waiting a packet of response sent by the peer target.
        /// </summary>
        private async Task EnsurePeerReceiveLoop()
        {
            // TrySendPacketToPeerTarget replaces the operation token between
            // requests. Let the previous reader observe cancellation before a new
            // reader is started on the same NetworkStream.
            if (_peerReceiveTask != null && !_peerReceiveTask.IsCompleted)
            {
                try { await _peerReceiveTask.ConfigureAwait(false); }
                catch { /* The receive loop reports socket failures itself. */ }
            }

            if (_peerCancellationTokenMain == null ||
                _peerCancellationTokenMain.IsCancellationRequested ||
                !PeerConnectStatus)
                return;

            CancellationTokenSource receiveCancellation = _peerCancellationTokenMain;
            _peerReceiveTask = Task.Run(() => TaskWaitPeerPacketResponse(receiveCancellation));
        }

        /// <summary>
        /// The sole reader for this peer socket. It remains active while the
        /// connection is kept alive, so unsolicited keep-alive replies cannot
        /// remain queued in front of the next request/response exchange.
        /// </summary>
        private async Task TaskWaitPeerPacketResponse(CancellationTokenSource receiveCancellation)
        {
            CancellationToken cancellationToken = receiveCancellation.Token;
            while (!cancellationToken.IsCancellationRequested && PeerConnectStatus)
            {
                BinaryPacket binaryPacket = await _peerSocketClient.ReceiveBinaryPacketAsync(
                    _peerNetworkSetting.PeerApiMaxConnectionDelay * 1000,
                    cancellationToken).ConfigureAwait(false);

                if (binaryPacket == null)
                {
                    // Receive timeout before the first byte is recoverable. A real
                    // disconnect is detected by IsConnected() and exits the loop.
                    if (cancellationToken.IsCancellationRequested || !PeerConnectStatus)
                        break;
                    continue;
                }

                if (binaryPacket.Data == null || binaryPacket.Data.Length == 0)
                    continue;

                ClassPeerCheckManager.UpdatePeerClientLastPacketReceived(
                    _peerDatabase, PeerIpTarget, PeerUniqueIdTarget,
                    TaskManager.TaskManager.CurrentTimestampSecond, receiveCancellation);

                ClassPeerPacketRecvObject peerPacketReceived =
                    new ClassPeerPacketRecvObject(binaryPacket.Data, out bool status);

                if (!status || peerPacketReceived == null)
                {
#if DEBUG
                    Debug.WriteLine("Can't build binary packet data from: " + _peerSocketClient.GetIp);
#endif
                    _peerSocketClient.Kill(SocketShutdown.Both);
                    break;
                }

                PeerPacketTypeReceived = peerPacketReceived.PacketOrder;
                TaskCompletionSource<bool> pending;
                lock (_peerResponseLock)
                    pending = _peerPendingResponse;

                bool isRequestResponse = pending != null &&
                    (peerPacketReceived.PacketOrder == PacketResponseExpected ||
                     peerPacketReceived.PacketOrder == ClassPeerEnumPacketResponse.NOT_YET_SYNCED ||
                     peerPacketReceived.PacketOrder == ClassPeerEnumPacketResponse.SEND_MISSING_AUTH_KEYS);

                if (isRequestResponse)
                {
                    if (peerPacketReceived.PacketOrder == PacketResponseExpected &&
                        _peerDatabase.ContainsPeerUniqueId(PeerIpTarget, PeerUniqueIdTarget, receiveCancellation))
                    {
                        _peerDatabase[PeerIpTarget, PeerUniqueIdTarget, receiveCancellation]
                            .PeerTimestampSignatureWhitelist = peerPacketReceived.PeerLastTimestampSignatureWhitelist;
                    }

                    PeerPacketReceived = peerPacketReceived;
                    PeerPacketReceivedStatus = true;

                    lock (_peerResponseLock)
                    {
                        if (ReferenceEquals(_peerPendingResponse, pending))
                            _peerPendingResponse = null;
                    }
                    pending.TrySetResult(true);
                }
                else if (PacketResponseExpected == ClassPeerEnumPacketResponse.SEND_BLOCK_DATA)
                {
#if DEBUG
                    Debug.WriteLine("Unexpected packet data received: " + JsonConvert.SerializeObject(peerPacketReceived));
#endif
                    ClassPeerCheckManager.InputPeerClientInvalidPacket(
                        _peerDatabase, PeerIpTarget, PeerUniqueIdTarget,
                        _peerNetworkSetting, _peerFirewallSettingObject, receiveCancellation);
                }
            }

            PeerTaskStatus = false;
        }

        #endregion

        #region Enable Keep alive functions.

        /// <summary>
        /// Enable a task who send a packet of keep alive to the peer target.
        /// </summary>
        private void TaskEnablePeerPacketKeepAlive()
        {

            CancelKeepAlive();

            var peerObject = _peerDatabase[PeerIpTarget, PeerUniqueIdTarget, _peerCancellationTokenMain];

            if (peerObject != null)
            {
                _peerTaskKeepAliveStatus = true;

                _peerCancellationTokenKeepAlive =
                    CancellationTokenSource.CreateLinkedTokenSource(
                        _peerCancellationTokenMain.Token);

                TaskManager.TaskManager.InsertTask(new Func<Task>(async () =>
                {

                    var sendObject = new ClassPeerPacketSendObject(_peerNetworkSetting.PeerUniqueId, peerObject.PeerInternPublicKey, peerObject.PeerClientLastTimestampPeerPacketSignatureWhitelist)
                    {
                        PacketOrder = ClassPeerEnumPacketSend.ASK_KEEP_ALIVE,
                        PacketContent = ClassUtility.SerializeData(new ClassPeerPacketAskKeepAlive()
                        {
                            PacketTimestamp = TaskManager.TaskManager.CurrentTimestampSecond
                        }),
                    };


                    while (PeerConnectStatus &&
                           _peerTaskKeepAliveStatus &&
                           !_peerCancellationTokenKeepAlive.IsCancellationRequested)
                    {
                        try
                        {

                            sendObject.PacketContent = ClassUtility.SerializeData(new ClassPeerPacketAskKeepAlive()
                            {
                                PacketTimestamp = TaskManager.TaskManager.CurrentTimestampSecond
                            });

                            if (!await SendPeerPacket(sendObject.GetPacketData(), _peerCancellationTokenKeepAlive))
                            {
                                _peerTaskKeepAliveStatus = false;
                                break;
                            }

                            await Task.Delay(
                                5 * 1000,
                                _peerCancellationTokenKeepAlive.Token);
                        }
                        catch (Exception e)
                        {
                            if (e is SocketException || e is TaskCanceledException)
                            {
                                _peerTaskKeepAliveStatus = false;
                                break;
                            }
                        }

                    }

                }), 0, _peerCancellationTokenKeepAlive, null).Wait();
            }
        }



        #endregion

        #region Manage TCP Connection.

        /// <summary>
        /// Send a packet to the peer target.
        /// </summary>
        /// <param name="packet"></param>
        /// <param name="cancellation"></param>
        /// <returns></returns>
        private async Task<bool> SendPeerPacket(byte[] packet, CancellationTokenSource cancellation)
        {
            if (packet == null ||
                packet.Length == 0 ||
                cancellation == null ||
                cancellation.IsCancellationRequested ||
                _peerSocketClient == null)
                return false;

            bool lockTaken = false;

            try
            {
                await _peerSendLock.WaitAsync().ConfigureAwait(false);
                lockTaken = true;

                if (cancellation.IsCancellationRequested ||
                    !_peerSocketClient.IsConnected())
                    return false;

                return await _peerSocketClient.TrySendSplittedPacket(
                    0, 0, unchecked((uint)Interlocked.Increment(ref _binaryPacketId)),
                    packet, cancellation.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return false;
            }
            catch (ObjectDisposedException)
            {
                return false;
            }
            finally
            {
                if (lockTaken)
                    _peerSendLock.Release();
            }
        }

        private void CancelHandlePacket()
        {
            PeerTaskStatus = false;
            PeerPacketReceivedStatus = false;
            PeerPacketReceived = null;
            PeerPacketTypeReceived = ClassPeerEnumPacketResponse.NONE;
        }

        /// <summary>
        /// Cancel keep alive.
        /// </summary>
        private void CancelKeepAlive()
        {

            try
            {
                if (_peerCancellationTokenKeepAlive != null)
                {
                    if (!_peerCancellationTokenKeepAlive.IsCancellationRequested)
                        _peerCancellationTokenKeepAlive.Cancel();
                }
            }
            catch
            {

            }

            _peerTaskKeepAliveStatus = false;

        }

        /// <summary>
        /// Disconnect from target.
        /// </summary>
        public void DisconnectFromTarget()
        {
            _peerSocketClient?.Kill(SocketShutdown.Both);
        }



        #endregion
    }
}
