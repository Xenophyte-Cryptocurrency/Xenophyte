using Xenophyte_Lib.Blockchain.Transaction.Object;
using System.Collections.Generic;

namespace Xenophyte_Lib.Instance.Node.Network.Services.P2P.Sync.Packet.SubPacket.Request
{
    public class ClassPeerPacketSendAskMemPoolTransactionVote
    {
        public List<ClassTransactionObject> ListTransactionObject;
        public long PacketTimestamp;
    }
}
