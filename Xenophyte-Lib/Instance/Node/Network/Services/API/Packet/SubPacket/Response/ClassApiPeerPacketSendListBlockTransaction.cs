using Xenophyte_Lib.Blockchain.Block.Object.Structure;
using System.Collections.Generic;


namespace Xenophyte_Lib.Instance.Node.Network.Services.API.Packet.SubPacket.Response
{
    public class ClassApiPeerPacketSendListBlockTransaction
    {
        public List<ClassBlockTransaction> ListBlockTransaction;
        public long PacketTimestamp;
    }
}
