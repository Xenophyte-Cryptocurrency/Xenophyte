using Xenophyte_Lib.Other.Object.List;
using System.Collections.Generic;

namespace Xenophyte_Lib.Blockchain.Database.Memory.Main.Object
{
    public class ClassBlockchainBlockConfirmationResultObject
    {
        public bool Status;
        public long LastBlockHeightConfirmationDone;
        public DisposableList<long> ListBlockHeightConfirmed;

        public ClassBlockchainBlockConfirmationResultObject()
        {
            ListBlockHeightConfirmed = new DisposableList<long>();
        }
    }
}
