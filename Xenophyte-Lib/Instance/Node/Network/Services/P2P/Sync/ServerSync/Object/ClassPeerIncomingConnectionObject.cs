using System.Collections.Concurrent;
using System.Threading;
using Xenophyte_Lib.Instance.Node.Network.Services.P2P.Sync.ServerSync.Client;
using Xenophyte_Lib.Instance.Node.Setting.Object;

namespace Xenophyte_Lib.Instance.Node.Network.Services.P2P.Sync.ServerSync.Object
{
    public class ClassPeerIncomingConnectionObject
    {
        public SemaphoreSlim SemaphoreHandleConnection;
        public ConcurrentDictionary<long, ClassPeerNetworkClientServerObject> ListPeerClientObject;
        public bool OnCleanUp;

        public ClassPeerIncomingConnectionObject(ClassPeerNetworkSettingObject peerNetworkSettingObject)
        {
            SemaphoreHandleConnection = new SemaphoreSlim(1, peerNetworkSettingObject.PeerMaxThreadsPool * peerNetworkSettingObject.PeerMaxThreadsPool);
            ListPeerClientObject = new ConcurrentDictionary<long, ClassPeerNetworkClientServerObject>();
            OnCleanUp = false;
        }
    }
}
