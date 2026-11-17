using System.Collections.Concurrent;
using System.Threading;
using Xenophyte_Lib.Instance.Node.Network.Services.API.Client;
using Xenophyte_Lib.Utility;

namespace Xenophyte_Lib.Instance.Node.Network.Services.API.Server.Object
{
    public class ClassPeerApiIncomingConnectionObject
    {
        public ConcurrentDictionary<long, ClassPeerApiClientObject> ListApiClientObject;
        public SemaphoreSlim SemaphoreHandleConnection;

        public ClassPeerApiIncomingConnectionObject()
        {
            SemaphoreHandleConnection = new SemaphoreSlim(1, ClassUtility.GetMaxAvailableProcessorCount() * ClassUtility.GetMaxAvailableProcessorCount());
            ListApiClientObject = new ConcurrentDictionary<long, ClassPeerApiClientObject>();
        }
    }
}
