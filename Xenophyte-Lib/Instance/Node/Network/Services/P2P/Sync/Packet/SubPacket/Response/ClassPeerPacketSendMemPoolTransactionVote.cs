using Xenophyte_Lib.Blockchain.Transaction.Enum;
using System.Collections.Generic;

namespace Xenophyte_Lib.Instance.Node.Network.Services.P2P.Sync.Packet.SubPacket.Response
{
    public class ClassPeerPacketSendMemPoolTransactionVote
    {
        public Dictionary<string, ClassTransactionEnumStatus> ListTransactionHashResult;
        public long PacketTimestamp;
        public string PacketNumericHash;
        public string PacketNumericSignature;
    }
}
