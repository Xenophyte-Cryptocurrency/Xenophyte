using System;
using System.Collections.Concurrent;
#if DEBUG
using System.Diagnostics;
#endif
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Xenophyte_Lib.Instance.Node.Network.Database;
using Xenophyte_Lib.Instance.Node.Network.Services.Firewall.Manager;
using Xenophyte_Lib.Instance.Node.Network.Services.P2P.Sync.ServerSync.Client;
using Xenophyte_Lib.Instance.Node.Network.Services.P2P.Sync.ServerSync.Object;
using Xenophyte_Lib.Instance.Node.Network.Services.P2P.Sync.ServerSync.Service.Enum;
using Xenophyte_Lib.Instance.Node.Setting.Object;
using Xenophyte_Lib.Log;
using Xenophyte_Lib.Other.Object.List;
using Xenophyte_Lib.Other.Object.Network;
using Xenophyte_Lib.Utility;

namespace Xenophyte_Lib.Instance.Node.Network.Services.P2P.Sync.ServerSync.Service
{
    public class ClassPeerNetworkSyncServerObject : IDisposable
    {
        public bool NetworkPeerServerStatus;
        private TcpListener _tcpListenerPeer;
        private CancellationTokenSource _cancellationTokenSourcePeerServer;
        private ConcurrentDictionary<string, ClassPeerIncomingConnectionObject> _listPeerIncomingConnectionObject;
        public string PeerIpOpenNatServer;
        private ClassPeerDatabase _peerDatabase;
        private ClassPeerNetworkSettingObject _peerNetworkSettingObject;
        private ClassPeerFirewallSettingObject _firewallSettingObject;

        #region Dispose functions

        private bool _disposed;

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

            if (disposing)
            {
                StopPeerServer();
            }

            _disposed = true;
        }

        #endregion

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="peerIpOpenNatServer">The Public IP of the node, permit to ignore it on sync.</param>
        /// <param name="peerNetworkSettingObject">The network setting object.</param>
        /// <param name="firewallSettingObject">The firewall setting object.</param>
        public ClassPeerNetworkSyncServerObject(ClassPeerDatabase peerDatabase, string peerIpOpenNatServer, ClassPeerNetworkSettingObject peerNetworkSettingObject, ClassPeerFirewallSettingObject firewallSettingObject)
        {
            if (peerDatabase == null)
                throw new ArgumentNullException(nameof(peerDatabase));
            if (peerNetworkSettingObject == null)
                throw new ArgumentNullException(nameof(peerNetworkSettingObject));
            if (firewallSettingObject == null)
                throw new ArgumentNullException(nameof(firewallSettingObject));

            _peerDatabase = peerDatabase;
            PeerIpOpenNatServer = peerIpOpenNatServer;
            _peerNetworkSettingObject = peerNetworkSettingObject;
            _firewallSettingObject = firewallSettingObject;
            _listPeerIncomingConnectionObject = new ConcurrentDictionary<string, ClassPeerIncomingConnectionObject>();
        }

        #region Peer Server management functions.

        /// <summary>
        /// Start the peer server listening task.
        /// </summary>
        /// <returns>Return if the binding and the execution of listening incoming connection work.</returns>
        public bool StartPeerServer()
        {

            if (_disposed || NetworkPeerServerStatus)
                return false;

            CleanUpAllIncomingConnection();

            try
            {
                _tcpListenerPeer = new TcpListener(IPAddress.Parse(_peerNetworkSettingObject.ListenIp), _peerNetworkSettingObject.ListenPort);
                _tcpListenerPeer.Start(_peerNetworkSettingObject.PeerMaxNodeConnectionPerIp);
            }
            catch (Exception error)
            {
                ClassLog.WriteLine("Error on initialize TCP Listener of the Peer. | Exception: " + error.Message, ClassEnumLogLevelType.LOG_LEVEL_PEER_SERVER, ClassEnumLogWriteLevel.LOG_WRITE_LEVEL_MANDATORY_PRIORITY);
                return false;
            }

            _cancellationTokenSourcePeerServer = new CancellationTokenSource();
            NetworkPeerServerStatus = true;

            TaskManager.TaskManager.InsertTask(new Func<Task>(async () =>
            {
                while (NetworkPeerServerStatus)
                {
                    ClassCustomSocket clientPeerTcp = null;
                    try
                    {
                        clientPeerTcp = new ClassCustomSocket(await _tcpListenerPeer.AcceptSocketAsync(), true);

                        string clientIp = clientPeerTcp.GetIp;

                        await TaskManager.TaskManager.InsertTask(new Func<Task>(async () =>
                        {
                            ClassLog.WriteLine("Handle incoming connection from: " + clientIp, ClassEnumLogLevelType.LOG_LEVEL_PEER_SERVER, ClassEnumLogWriteLevel.LOG_WRITE_LEVEL_MANDATORY_PRIORITY, false, ConsoleColor.Green);

                            var handleResult = await HandleIncomingConnection(clientIp, clientPeerTcp, PeerIpOpenNatServer);

                            switch (handleResult)
                            {


#if NET5_0_OR_GREATER
                                case not ClassPeerNetworkServerHandleConnectionEnum.VALID_HANDLE:

#else
                                    case ClassPeerNetworkServerHandleConnectionEnum.BAD_CLIENT_STATUS:
                                    case ClassPeerNetworkServerHandleConnectionEnum.HANDLE_CLIENT_EXCEPTION:
                                    case ClassPeerNetworkServerHandleConnectionEnum.INSERT_CLIENT_IP_EXCEPTION:
                                    case ClassPeerNetworkServerHandleConnectionEnum.TOO_MUCH_ACTIVE_CONNECTION_CLIENT:
                                    case ClassPeerNetworkServerHandleConnectionEnum.BAD_RANDOM_ID:
                                    case ClassPeerNetworkServerHandleConnectionEnum.INSERT_CLIENT_EXCEPTION:
#endif

                                    {
                                        ClassLog.WriteLine("Cannot handle incoming connection from: " + clientIp + " | Result: " + System.Enum.GetName(typeof(ClassPeerNetworkServerHandleConnectionEnum), handleResult), ClassEnumLogLevelType.LOG_LEVEL_PEER_SERVER, ClassEnumLogWriteLevel.LOG_WRITE_LEVEL_MANDATORY_PRIORITY, false, ConsoleColor.Red);
                                        if (_firewallSettingObject.PeerEnableFirewallLink)
                                            ClassPeerFirewallManager.InsertInvalidPacket(clientIp);
                                        clientPeerTcp.Kill(SocketShutdown.Both);
                                    }
                                    break;
                            }
                        }), 0, _cancellationTokenSourcePeerServer, null);
                    }
                    catch
                    {
                        // A socket accepted just before a scheduling failure must not leak.
                        try { clientPeerTcp?.Kill(SocketShutdown.Both); } catch { }

                        // Listener shutdown is expected during cancellation.
                        if (!NetworkPeerServerStatus)
                            break;
                    }
                }
            }), 0, _cancellationTokenSourcePeerServer).Wait();


            return true;
        }

        /// <summary>
        /// Stop the peer server listening.
        /// </summary>
        public void StopPeerServer()
        {
            NetworkPeerServerStatus = false;
            try
            {
                try
                {
                    if (_cancellationTokenSourcePeerServer != null)
                    {
                        if (!_cancellationTokenSourcePeerServer.IsCancellationRequested)
                            _cancellationTokenSourcePeerServer.Cancel();
                    }
                }
                catch
                {
                    // Ignored.
                }
                try
                {
                    _tcpListenerPeer.Stop();
                }
                catch
                {
                    // Ignored.
                }

            }
            finally
            {
                CleanUpAllIncomingConnection();
                // Do not dispose the token source here: accepted connection tasks may
                // still be unwinding and may still be reading its token.
                _tcpListenerPeer = null;
            }
        }

        /// <summary>
        /// Handle incoming connection.
        /// </summary>
        /// <param name="clientIp">The ip of the incoming connection.</param>
        /// <param name="clientPeerTcp">The tcp client object of the incoming connection.</param>
        /// <param name="peerIpOpenNatServer">The public ip of the server.</param>
        /// <returns>Return the handling status of the incoming connection.</returns>
        private async Task<ClassPeerNetworkServerHandleConnectionEnum> HandleIncomingConnection(string clientIp, ClassCustomSocket clientPeerTcp, string peerIpOpenNatServer)
        {
            long randomId = -1;
            ClassPeerIncomingConnectionObject incomingConnections = null;
            ClassPeerNetworkClientServerObject peerClient = null;
            bool semaphoreAcquired = false;
            bool handlerCompleted = false;

            try
            {
                if (clientPeerTcp == null || string.IsNullOrEmpty(clientIp) ||
                    _cancellationTokenSourcePeerServer == null ||
                    _cancellationTokenSourcePeerServer.IsCancellationRequested)
                    return ClassPeerNetworkServerHandleConnectionEnum.HANDLE_CLIENT_EXCEPTION;

                if (_peerNetworkSettingObject.ListenIp != clientIp && _firewallSettingObject.PeerEnableFirewallLink &&
                    !ClassPeerFirewallManager.CheckClientIpStatus(clientIp))
                    return ClassPeerNetworkServerHandleConnectionEnum.BAD_CLIENT_STATUS;

                if (GetTotalActiveConnection(clientIp, _cancellationTokenSourcePeerServer) >=
                    _peerNetworkSettingObject.PeerMaxNodeConnectionPerIp)
                    return ClassPeerNetworkServerHandleConnectionEnum.TOO_MUCH_ACTIVE_CONNECTION_CLIENT;

                incomingConnections = _listPeerIncomingConnectionObject.GetOrAdd(
                    clientIp, _ => new ClassPeerIncomingConnectionObject(_peerNetworkSettingObject));

                randomId = await GeneratePeerUniqueIdAsync(clientPeerTcp, _cancellationTokenSourcePeerServer)
                    .ConfigureAwait(false);

                if (randomId < 0)
                    return ClassPeerNetworkServerHandleConnectionEnum.BAD_RANDOM_ID;

                peerClient = new ClassPeerNetworkClientServerObject(
                    _peerDatabase, clientPeerTcp, _cancellationTokenSourcePeerServer,
                    clientIp, peerIpOpenNatServer, _peerNetworkSettingObject, _firewallSettingObject);

                if (!incomingConnections.ListPeerClientObject.TryAdd(randomId, peerClient))
                    return ClassPeerNetworkServerHandleConnectionEnum.INSERT_CLIENT_EXCEPTION;

                // Use one linked token source and dispose it deterministically.
                using (CancellationTokenSource timeoutSource =
                    new CancellationTokenSource(_peerNetworkSettingObject.PeerMaxSemaphoreConnectAwaitDelay))
                using (CancellationTokenSource linkedSource =
                    CancellationTokenSource.CreateLinkedTokenSource(
                        _cancellationTokenSourcePeerServer.Token, timeoutSource.Token))
                {
                    semaphoreAcquired = await incomingConnections.SemaphoreHandleConnection.TryWaitAsync(
                        _peerNetworkSettingObject.PeerMaxSemaphoreConnectAwaitDelay, linkedSource)
                        .ConfigureAwait(false);

                    if (!semaphoreAcquired)
                        return ClassPeerNetworkServerHandleConnectionEnum.TOO_MUCH_ACTIVE_CONNECTION_CLIENT;

                    await peerClient.HandlePeerClient().ConfigureAwait(false);
                }

                handlerCompleted = true;
                return ClassPeerNetworkServerHandleConnectionEnum.VALID_HANDLE;
            }
            catch (OperationCanceledException)
            {
                return ClassPeerNetworkServerHandleConnectionEnum.HANDLE_CLIENT_EXCEPTION;
            }
            catch (Exception error)
            {
                ClassLog.WriteLine("Cannot handle incoming connection from: " + clientIp +
                    " | Exception: " + error.Message, ClassEnumLogLevelType.LOG_LEVEL_PEER_SERVER,
                    ClassEnumLogWriteLevel.LOG_WRITE_LEVEL_MANDATORY_PRIORITY, false, ConsoleColor.Red);

                return ClassPeerNetworkServerHandleConnectionEnum.HANDLE_CLIENT_EXCEPTION;
            }
            finally
            {
                if (semaphoreAcquired && incomingConnections != null)
                {
                    try { incomingConnections.SemaphoreHandleConnection.Release(); }
                    catch { /* Semaphore may already be disposed during server shutdown. */ }
                }

                // A client is retained only while its handler is successfully running/completed.
                if (peerClient != null && !handlerCompleted)
                {
                    if (incomingConnections != null && randomId >= 0)
                        incomingConnections.ListPeerClientObject.TryRemove(randomId, out _);
                    try { peerClient.Dispose(); } catch { }
                }
            }
        }

        #endregion

        #region Stats connections functions.

        /// <summary>
        /// Generate peer unique id.
        /// </summary>
        /// <param name="client"></param>
        /// <param name="cancellation"></param>
        /// <returns></returns>
        public async Task<long> GeneratePeerUniqueIdAsync(ClassCustomSocket client, CancellationTokenSource cancellation)
        {
            if (client == null || cancellation == null || _listPeerIncomingConnectionObject == null)
                return -1;

            try
            {
                while (!cancellation.IsCancellationRequested && client.IsConnected())
                {
                    if (!_listPeerIncomingConnectionObject.TryGetValue(client.GetIp, out ClassPeerIncomingConnectionObject incoming))
                        return -1;

                    long randomId = ClassUtility.GetRandomBetweenInt(0, int.MaxValue - 1);
                    if (!incoming.ListPeerClientObject.ContainsKey(randomId))
                        return randomId;

                    await Task.Delay(1, cancellation.Token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                return -1;
            }
            catch
            {
                return -1;
            }

            return -1;
        }

        /// <summary>
        /// Get the total active connection amount of a client ip.
        /// </summary>
        /// <param name="clientIp">The client IP.</param>
        /// <returns>Return the amount of total connection of a client IP.</returns>
        private int GetTotalActiveConnection(string clientIp, CancellationTokenSource cancellation)
        {
            if (cancellation == null ||
                !_listPeerIncomingConnectionObject.TryGetValue(clientIp, out ClassPeerIncomingConnectionObject incoming))
                return 0;

            int totalActiveConnection = 0;
            foreach (var peerClient in incoming.ListPeerClientObject.Values)
            {
                if (cancellation.IsCancellationRequested)
                    break;

                try
                {
                    if (peerClient != null && peerClient.ClientPeerConnectionStatus)
                        totalActiveConnection++;
                }
                catch
                {
                    // A connection may be disposed while its status is being read.
                }
            }

            return totalActiveConnection;
        }

        /// <summary>
        /// Get the total of all active connections of all client ip's.
        /// </summary>
        /// <returns>Return the amount of total active connections.</returns>
        public long GetAllTotalActiveConnection()
        {
            long totalActiveConnection = 0;

            foreach (ClassPeerIncomingConnectionObject incoming in _listPeerIncomingConnectionObject.Values)
            {
                if (incoming == null)
                    continue;

                foreach (ClassPeerNetworkClientServerObject peerClient in incoming.ListPeerClientObject.Values)
                {
                    try
                    {
                        if (peerClient != null && peerClient.ClientPeerConnectionStatus)
                            totalActiveConnection++;
                    }
                    catch
                    {
                        // A connection may be disposed while its status is being read.
                    }
                }
            }

            return totalActiveConnection;
        }

        /// <summary>
        /// Clean up inactive connection from client ip target.
        /// </summary>
        /// <param name="clientIp">The client IP.</param>
        public int CleanUpInactiveConnectionFromClientIpTarget(string clientIp)
        {
            int totalRemoved = 0;

            if (string.IsNullOrEmpty(clientIp) ||
                !_listPeerIncomingConnectionObject.TryGetValue(clientIp, out ClassPeerIncomingConnectionObject incoming) ||
                incoming == null)
                return totalRemoved;

            long currentTimestamp = TaskManager.TaskManager.CurrentTimestampSecond;
            foreach (var pair in incoming.ListPeerClientObject.ToArray())
            {
                bool shouldRemove = pair.Value == null;

                if (!shouldRemove)
                {
                    try
                    {
                        shouldRemove = !pair.Value.ClientPeerConnectionStatus ||
                            pair.Value.ClientPeerLastPacketReceived + _peerNetworkSettingObject.PeerMaxDelayConnection < currentTimestamp;
                    }
                    catch
                    {
                        // Treat an unreadable/disposed connection as stale.
                        shouldRemove = true;
                    }
                }

                if (shouldRemove && incoming.ListPeerClientObject.TryRemove(pair.Key, out ClassPeerNetworkClientServerObject removed))
                {
                    try { removed?.Dispose(); } catch { }
                    totalRemoved++;
                }
            }

            return totalRemoved;
        }

        /// <summary>
        /// Clean up all incoming closed connection.
        /// </summary>
        /// <param name="totalIp">The number of ip's returned from the amount of closed connections cleaned.</param>
        /// <returns>Return the amount of total connection closed.</returns>
        public long CleanUpAllIncomingClosedConnection(out int totalIp)
        {
            long totalConnectionClosed = 0;
            totalIp = 0;

            foreach (var pair in _listPeerIncomingConnectionObject.ToArray())
            {
                ClassPeerIncomingConnectionObject incoming = pair.Value;
                if (incoming == null)
                {
                    _listPeerIncomingConnectionObject.TryRemove(pair.Key, out _);
                    continue;
                }

                long totalClosed = CleanUpInactiveConnectionFromClientIpTarget(pair.Key);
                if (totalClosed > 0)
                    totalIp++;
                totalConnectionClosed += totalClosed;

                if (incoming.ListPeerClientObject.IsEmpty)
                    _listPeerIncomingConnectionObject.TryRemove(pair.Key, out _);
            }

            return totalConnectionClosed;
        }

        /// <summary>
        /// Clean up all incoming connection.
        /// </summary>
        /// <returns>Return the amount of incoming connection closed.</returns>
        public long CleanUpAllIncomingConnection()
        {
            long totalConnectionClosed = 0;

            foreach (var pair in _listPeerIncomingConnectionObject.ToArray())
            {
                ClassPeerIncomingConnectionObject incoming = pair.Value;
                if (incoming == null)
                {
                    _listPeerIncomingConnectionObject.TryRemove(pair.Key, out _);
                    continue;
                }

                foreach (var clientPair in incoming.ListPeerClientObject.ToArray())
                {
                    if (incoming.ListPeerClientObject.TryRemove(clientPair.Key, out ClassPeerNetworkClientServerObject peerClient))
                    {
                        try
                        {
                            if (peerClient != null && peerClient.ClientPeerConnectionStatus)
                                totalConnectionClosed++;
                        }
                        catch { }

                        try { peerClient?.Dispose(); } catch { }
                    }
                }

                _listPeerIncomingConnectionObject.TryRemove(pair.Key, out _);
            }

            return totalConnectionClosed;
        }

        #endregion
    }
}
