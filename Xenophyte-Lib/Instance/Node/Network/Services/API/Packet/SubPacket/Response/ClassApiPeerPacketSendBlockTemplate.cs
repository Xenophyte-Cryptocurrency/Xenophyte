using System.Numerics;
using Xenophyte_Lib.Blockchain.Mining.Object;

namespace Xenophyte_Lib.Instance.Node.Network.Services.API.Packet.SubPacket.Response
{
    public class ClassApiPeerPacketSendBlockTemplate
    {
        public long CurrentBlockHeight;
        public BigInteger CurrentBlockDifficulty;
        public string CurrentBlockHash;
        public long LastTimestampBlockFound;
        public ClassMiningPoWaCSettingObject CurrentMiningPoWaCSetting;
        public long PacketTimestamp;
    }
}
