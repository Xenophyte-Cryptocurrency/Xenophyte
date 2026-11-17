using System.Collections.Generic;
using System.Numerics;
using Xenophyte_Lib.Blockchain.Transaction.Object;

namespace Xenophyte_Desktop_Wallet.MainForm.Object
{
    public class ClassSendTransactionFeeCostCalculationResultObject
    {
        public BigInteger TotalFeeCost;
        public BigInteger FeeConfirmationCost;
        public BigInteger FeeSizeCost;
        public Dictionary<string, ClassTransactionHashSourceObject> TransactionAmountSourceList;
        public bool Failed;

        public ClassSendTransactionFeeCostCalculationResultObject()
        {
            Failed = true; // Default.
            TransactionAmountSourceList = new Dictionary<string, ClassTransactionHashSourceObject>();
        }
    }
}
