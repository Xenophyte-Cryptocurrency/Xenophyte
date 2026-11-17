using Xenophyte_Lib.Instance.Node.Network.Services.P2P.Sync.Packet.SubPacket.Response.Enum;

namespace Xenophyte_Lib.Instance.Node.Network.Services.P2P.Sync.Packet.SubPacket.Response
{
    public class ClassPeerPacketSendMiningShareVote
    {
        public long BlockHeight;
        public ClassPeerPacketMiningShareVoteEnum VoteStatus;
        public long PacketTimestamp;
        public string PacketNumericHash;
        public string PacketNumericSignature;
    }
}
