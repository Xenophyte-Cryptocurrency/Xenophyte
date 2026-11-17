using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Xenophyte_Desktop_Wallet.Settings.Enum;
using Xenophyte_Lib.Blockchain.Database.DatabaseSetting;
using Xenophyte_Lib.Instance.Node.Network.Database.Manager;
using Xenophyte_Lib.Instance.Node.Setting.Object;
using Xenophyte_Lib.Utility;

namespace Xenophyte_Desktop_Wallet.Settings.Object
{
    public class ClassWalletSettingObject
    {
        public string WalletLanguageNameSelected;
        public string WalletDirectoryPath;
        public string WalletSyncCacheDirectoryPath;
        public string WalletSyncCacheFilePath;
        public ClassNodeSettingObject WalletInternalSyncNodeSetting;

        [JsonConverter(typeof(StringEnumConverter))]
        public ClassWalletSettingEnumSyncMode WalletSyncMode;
        public string ApiHost;
        public int ApiPort;
        public int ApiMaxRetry = 10;

        /// <summary>
        /// Constructor with default value.
        /// </summary>
        public ClassWalletSettingObject()
        {
            WalletLanguageNameSelected = ClassWalletDefaultSetting.DefaultLanguageName;
            WalletDirectoryPath = ClassUtility.ConvertPath(AppContext.BaseDirectory + ClassWalletDefaultSetting.DefaultWalletDirectoryFilePath);
            WalletSyncCacheDirectoryPath = ClassUtility.ConvertPath(AppContext.BaseDirectory + ClassWalletDefaultSetting.WalletDefaultSyncCacheDirectoryPath);
            WalletSyncCacheFilePath = WalletSyncCacheDirectoryPath + ClassWalletDefaultSetting.WalletDefaultSyncCacheFilename;
            ApiHost = null;
            ApiPort = 0;
            WalletSyncMode = ClassWalletSettingEnumSyncMode.INTERNAL_PEER_SYNC_MODE;
            string numericPrivateKey = ClassPeerKeysManager.GeneratePeerPrivateKey();

            WalletInternalSyncNodeSetting = new ClassNodeSettingObject()
            {

                PeerNetworkSettingObject =
                    new ClassPeerNetworkSettingObject()
                    {
                        ListenIp = "127.0.0.1",
                        PublicPeer = false,
                        PeerNumericPrivateKey = numericPrivateKey,
                        PeerNumericPublicKey = ClassPeerKeysManager.GeneratePeerPublicKeyFromPrivateKey(numericPrivateKey),
                        PeerUniqueId = ClassPeerKeysManager.GeneratePeerUniqueId()
                    },
                PeerBlockchainDatabaseSettingObject = new ClassBlockchainDatabaseSetting(),
                PeerLogSettingObject = new ClassPeerLogSettingObject(),
                PeerFirewallSettingObject = new ClassPeerFirewallSettingObject()
                {
                    PeerEnableFirewallLink = false
                }
            };
        }
    }
}
