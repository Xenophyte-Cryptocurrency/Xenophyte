using Xenophyte_Lib.Blockchain.Block.Object.Structure;
using System.Collections.Generic;

namespace Xenophyte_RPC_Wallet.API.Service.Packet.Object.Response.POST
{
    public class ClassRpcApiSendWalletTransaction
    {
        public List<ClassBlockTransaction> block_transaction_object;
        public long packet_timestamp;
    }
}
