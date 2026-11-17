using System.Numerics;
using Xenophyte_Lib.Blockchain.Checkpoint.Enum;

namespace Xenophyte_Lib.Blockchain.Checkpoint.Object
{
    public class ClassCheckpointObject
    {
        /// <summary>
        /// Type.
        /// </summary>
        public ClassCheckpointEnumType CheckpointType;

        /// <summary>
        /// Used by blockchain tx confirmations checkpoints.
        /// </summary>
        public long BlockHeight;

        /// <summary>
        /// Used by wallet checkpoint.
        /// </summary>
        public string PossibleWalletAddress;
        public BigInteger PossibleValue;
        public BigInteger PossibleValue2;
    }
}
