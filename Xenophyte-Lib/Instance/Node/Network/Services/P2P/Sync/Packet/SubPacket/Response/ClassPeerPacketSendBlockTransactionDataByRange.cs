using System.Collections.Generic;
using Xenophyte_Lib.Blockchain.Transaction.Object;

namespace Xenophyte_Lib.Instance.Node.Network.Services.P2P.Sync.Packet.SubPacket.Response
{
    public class ClassPeerPacketSendBlockTransactionDataByRange
    {
        public long BlockHeight;
        public Dictionary<string, ClassTransactionObject> ListTransactionObject;
        public long PacketTimestamp;
        public string PacketNumericHash;
        public string PacketNumericSignature;
    }
}
