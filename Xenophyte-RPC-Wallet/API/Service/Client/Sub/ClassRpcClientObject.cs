using Xenophyte_Lib.Utility;
using System.Collections.Generic;
using System.Threading;

namespace Xenophyte_RPC_Wallet.API.Service.Client.Sub
{
    public class ClassRpcClientObject
    {
        public SemaphoreSlim _semaphoreHandleRpcClient;
        public List<ClassRpcApiClient> _listRpcClientObject;


        /// <summary>
        /// Constructor.
        /// </summary>
        public ClassRpcClientObject()
        {
            _semaphoreHandleRpcClient = new SemaphoreSlim(1, ClassUtility.GetMaxAvailableProcessorCount());
            _listRpcClientObject = new List<ClassRpcApiClient>();
        }
    }
}
