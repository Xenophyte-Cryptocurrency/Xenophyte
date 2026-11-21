using Xenophyte_Lib.Blockchain.Block.Function;
using Xenophyte_Lib.Blockchain.Block.Object.Structure;
using Xenophyte_Lib.Blockchain.Database.DatabaseSetting;
using Xenophyte_Lib.Blockchain.Database.Memory.Cache.Object.Systems.IO.Disk.Function;
using Xenophyte_Lib.Blockchain.Database.Memory.Cache.Object.Systems.IO.Main;
using Xenophyte_Lib.Blockchain.Setting;
using Xenophyte_Lib.Log;
using Xenophyte_Lib.Other.Object.List;
using Xenophyte_Lib.Utility;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Xenophyte_Lib.Blockchain.Database.Memory.Cache.Object.Systems.IO.Disk.Object
{
    public class ClassCacheIoIndexObject : ClassCacheIoFunction
    {

        /// <summary>
        /// The controller object, usefull for calculate the memory usage across multiple io cache files.
        /// </summary>
        private ClassCacheIoSystem _ioCacheSystem;

        /// <summary>
        /// Contains every IO Data and their structures.
        /// </summary>
        private Dictionary<long, ClassCacheIoStructureObject> _ioStructureObjectsDictionary;

        /// <summary>
        /// IO Streams and files paths.
        /// </summary>
        private bool _ioStreamsClosed;
        // Volatile provides visibility to the wait loop; public cache operations are
        // additionally serialized by _ioSemaphoreAccess.
        private volatile bool _ioOnWriteData;
        private FileStream _ioDataStructureFileLockStream;
        private string _ioDataStructureFilename;
        private string _ioDataStructureFilePath;
        private UTF8Encoding _ioDataUtf8Encoding;
        private long _lastIoCacheFileLength;
        private long _totalIoCacheFileSize;
        private long _lastIoCacheTotalFileSizeWritten;

        /// <summary>
        /// Lock multithreading access.
        /// </summary>
        private SemaphoreSlim _ioSemaphoreAccess;

        /// <summary>
        /// Blockchain database settings.
        /// </summary>
        private ClassBlockchainDatabaseSetting _blockchainDatabaseSetting;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="ioDataStructureFilename"></param>
        /// <param name="blockchainDatabaseSetting"></param>
        /// <param name="ioCacheSystem"></param>
        public ClassCacheIoIndexObject(string ioDataStructureFilename, ClassBlockchainDatabaseSetting blockchainDatabaseSetting, ClassCacheIoSystem ioCacheSystem)
        {
            _ioCacheSystem = ioCacheSystem;
            _blockchainDatabaseSetting = blockchainDatabaseSetting;
            _ioDataStructureFilename = ioDataStructureFilename;
            _ioDataStructureFilePath = blockchainDatabaseSetting.GetBlockchainCacheDirectoryPath + ioDataStructureFilename;
            _ioStructureObjectsDictionary = new Dictionary<long, ClassCacheIoStructureObject>();
            _ioSemaphoreAccess = new SemaphoreSlim(1, 1);
            _ioDataUtf8Encoding = new UTF8Encoding(true, false);
        }

        #region Initialize Io Cache Index functions.

        /// <summary>
        /// Initialize the io cache object.
        /// </summary>
        /// <returns></returns>
        public async Task<Tuple<bool, HashSet<long>>> InitializeIoCacheObjectAsync()
        {
            if (!File.Exists(_ioDataStructureFilePath))
                File.Create(_ioDataStructureFilePath).Close();

            _ioDataStructureFileLockStream = new FileStream(_ioDataStructureFilePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, _blockchainDatabaseSetting.BlockchainCacheSetting.IoCacheDiskReadStreamBufferSize, FileOptions.Asynchronous | FileOptions.RandomAccess);

            return File.Exists(_ioDataStructureFilePath) ? await RunIoDataIndexingAsync() : new Tuple<bool, HashSet<long>>(true, new HashSet<long>());
        }

        /// <summary>
        /// Run io data indexing.
        /// </summary>
        /// <returns></returns>
        private async Task<Tuple<bool, HashSet<long>>> RunIoDataIndexingAsync()
        {
            await ResetSeekLockStreamAsync(null);

            using (StreamReader reader = new StreamReader(_ioDataStructureFileLockStream, _ioDataUtf8Encoding, false, _blockchainDatabaseSetting.BlockchainCacheSetting.IoCacheDiskReadStreamBufferSize, true))
            {
                string ioDataLine;

                long currentStreamPosition = 0;

                while ((ioDataLine = reader.ReadLine()) != null)
                {
                    if (ioDataLine.StartsWith(IoDataBeginBlockString))
                    {
                        long blockHeight = ExtractBlockHeight(ioDataLine);

                        if (blockHeight > BlockchainSetting.GenesisBlockHeight)
                        {

                            #region Retrieve important informations from the block.

                            string ioDataMerged = ioDataLine + Environment.NewLine;

                            bool complete = false;

                            while (true)
                            {
                                ioDataLine = reader.ReadLine();

                                if (ioDataLine == null)
                                    break;

                                if (ioDataLine.StartsWith(IoDataEndBlockString))
                                {
                                    ioDataMerged += ioDataLine + Environment.NewLine;
                                    complete = true;
                                    break;
                                }

                                ioDataMerged += ioDataLine + Environment.NewLine;
                            }

                            #endregion

                            if (complete)
                            {
                                // Test the block object read.
                                if (IoStringDataLineToBlockObject(ioDataMerged, _blockchainDatabaseSetting, false, null, out ClassBlockObject blockObject))
                                {
                                    if (blockObject?.BlockHeight == blockHeight)
                                    {
                                        if (!_ioStructureObjectsDictionary.ContainsKey(blockHeight))
                                        {
                                            try
                                            {
                                                _ioStructureObjectsDictionary.Add(blockHeight, new ClassCacheIoStructureObject()
                                                {
                                                    IsWritten = true,
                                                    IoDataPosition = currentStreamPosition,
                                                    IoDataSizeOnFile = ioDataMerged.Length,
                                                });

                                                if (await ReadAndGetIoBlockDataFromBlockHeightOnIoStreamFile(blockHeight, null, false) == null)
                                                    _ioStructureObjectsDictionary.Remove(blockHeight);
                                            }
                                            catch
                                            {
                                                // Ignored.
                                            }
                                        }
                                        else
                                        {
                                            long previousPosition = _ioStructureObjectsDictionary[blockHeight].IoDataPosition;
                                            long previousSize = _ioStructureObjectsDictionary[blockHeight].IoDataSizeOnFile;
                                            _ioStructureObjectsDictionary[blockHeight].IoDataPosition = currentStreamPosition;
                                            _ioStructureObjectsDictionary[blockHeight].IoDataSizeOnFile = ioDataMerged.Length;

                                            if (await ReadAndGetIoBlockDataFromBlockHeightOnIoStreamFile(blockHeight, null, false) == null)
                                            {
                                                _ioStructureObjectsDictionary[blockHeight].IoDataPosition = previousPosition;
                                                _ioStructureObjectsDictionary[blockHeight].IoDataSizeOnFile = previousSize;
                                            }
                                        }

                                    }
                                }
                            }
                        }
                    }

                    reader.DiscardBufferedData();
                    reader.BaseStream.Flush();
                    currentStreamPosition = reader.BaseStream.Position;
                }
            }


            return new Tuple<bool, HashSet<long>>(true, _ioStructureObjectsDictionary.Keys.ToHashSet());
        }

        #endregion

        #region Get/Set/Insert/Delete/Contains IO Data functions.

        #region Get IO Data functions.

        /// <summary>
        /// Get a list of block data information by a list of block height from the io cache file or from the memory.
        /// </summary>
        /// <param name="listBlockHeight"></param>
        /// <param name="cancellationIoCache"></param>
        /// <returns></returns>
        public async Task<List<ClassBlockObject>> GetIoListBlockDataInformationFromListBlockHeight(List<long> listBlockHeight, CancellationTokenSource cancellationIoCache)
        {
            List<ClassBlockObject> listBlockInformation = new List<ClassBlockObject>();

            bool useSemaphore = false;

            try
            {
                useSemaphore = await _ioSemaphoreAccess.TryWaitAsync(cancellationIoCache);

                if (!useSemaphore)
                    return listBlockInformation;

                foreach (long blockHeight in listBlockHeight)
                {
                    try
                    {
                        if (_ioStructureObjectsDictionary.ContainsKey(blockHeight))
                        {
                            if (!_ioStructureObjectsDictionary[blockHeight].IsNull)
                            {
                                _ioStructureObjectsDictionary[blockHeight].BlockObject.DeepCloneBlockObject(false, out ClassBlockObject blockObjectCopy);
                                listBlockInformation.Add(blockObjectCopy);
                            }
                            else
                            {
                                ClassBlockObject blockObject = await CallGetRetrieveDataAccess(blockHeight, false, true, false, cancellationIoCache);

                                if (blockObject != null)
                                    listBlockInformation.Add(blockObject);
                            }
                        }
                    }
                    catch
                    {
                        listBlockInformation.Clear();
                        break;
                    }
                }
            }
            finally
            {
                if (useSemaphore)
                    _ioSemaphoreAccess.Release();
            }

            return listBlockInformation;
        }

        /// <summary>
        /// Retrieve back only a io block data information object from a block height target.
        /// </summary>
        /// <param name="blockHeight"></param>
        /// <param name="cancellationIoCache"></param>
        /// <returns></returns>
        public async Task<ClassBlockObject> GetIoBlockDataInformationFromBlockHeight(long blockHeight, CancellationTokenSource cancellationIoCache)
        {
            ClassBlockObject blockObject = null;

            bool useSemaphore = false;

            try
            {
                useSemaphore = await _ioSemaphoreAccess.TryWaitAsync(cancellationIoCache);

                if (useSemaphore)
                    blockObject = await CallGetRetrieveDataAccess(blockHeight, false, true, false, cancellationIoCache);
            }
            finally
            {
                if (useSemaphore)
                    _ioSemaphoreAccess.Release();
            }

            return blockObject;
        }

        /// <summary>
        /// Retrieve back only the block transaction count from a block height target.
        /// </summary>
        /// <param name="blockHeight"></param>
        /// <param name="cancellationIoCache"></param>
        /// <returns></returns>
        public async Task<int> GetIoBlockTransactionCountFromBlockHeight(long blockHeight, CancellationTokenSource cancellationIoCache)
        {
            int blockTransactionCount = 0;

            bool useSemaphore = false;

            try
            {
                useSemaphore = await _ioSemaphoreAccess.TryWaitAsync(cancellationIoCache);

                if (!useSemaphore)
                    return blockTransactionCount;

                if (_ioStructureObjectsDictionary.ContainsKey(blockHeight))
                {

                    ClassBlockObject blockObject = await CallGetRetrieveDataAccess(blockHeight, false, true, false, cancellationIoCache);

                    if (blockObject != null)
                        blockTransactionCount = blockObject.TotalTransaction;
                }

            }
            finally
            {
                if (useSemaphore)
                    _ioSemaphoreAccess.Release();
            }

            return blockTransactionCount;
        }

        /// <summary>
        /// Retrieve back a io block data object from a block height target.
        /// </summary>
        /// <param name="blockHeight"></param>
        /// <param name="keepAlive"></param>
        /// <param name="cancellationIoCache"></param>
        /// <returns></returns>
        public async Task<ClassBlockObject> GetIoBlockDataFromBlockHeight(long blockHeight, bool keepAlive, bool clone, CancellationTokenSource cancellationIoCache)
        {
            ClassBlockObject blockObject = null;
            bool useSemaphore = false;

            try
            {

                useSemaphore = await _ioSemaphoreAccess.TryWaitAsync(cancellationIoCache);

                if (useSemaphore)
                    return await CallGetRetrieveDataAccess(blockHeight, keepAlive, false, clone, cancellationIoCache);
            }
            finally
            {
                if (useSemaphore)
                    _ioSemaphoreAccess.Release();
            }

            return blockObject;
        }

        /// <summary>
        /// Call a retrieve get data access.
        /// </summary>
        /// <param name="blockHeight"></param>
        /// <param name="keepAlive"></param>
        /// <param name="blockInformationsOnly"></param>
        /// <param name="cancellation"></param>
        /// <returns></returns>
        private async Task<ClassBlockObject> CallGetRetrieveDataAccess(long blockHeight, bool keepAlive, bool blockInformationsOnly, bool clone, CancellationTokenSource cancellation)
        {
            ClassBlockObject blockObject = null;

            if (_ioStructureObjectsDictionary.ContainsKey(blockHeight))
            {
                if (!_ioStructureObjectsDictionary[blockHeight].IsDeleted)
                {
                    if (!_ioStructureObjectsDictionary[blockHeight].IsNull)
                    {
                        if (!blockInformationsOnly)
                            blockObject = clone ? _ioStructureObjectsDictionary[blockHeight].BlockObject.DirectCloneBlockObject() : _ioStructureObjectsDictionary[blockHeight].BlockObject;
                        else
                            _ioStructureObjectsDictionary[blockHeight].BlockObject.DeepCloneBlockObject(false, out blockObject);
                    }
                    else
                    {
                        blockObject = await ReadAndGetIoBlockDataFromBlockHeightOnIoStreamFile(blockHeight, cancellation, blockInformationsOnly);

                        // Update the io cache file and remove the data updated from the active memory
                        if (blockObject != null && keepAlive && !blockInformationsOnly)
                            await InsertInActiveMemory(blockObject, keepAlive, true, cancellation);
                    }
                }
            }

            return blockObject;
        }

        /// <summary>
        /// Retrieve back a list of block object from the io cache object target by a list of block height.
        /// </summary>
        /// <param name="listBlockHeight">The list of block height target.</param>
        /// <param name="keepAlive">Keep alive or not data inside of the active memory.</param>
        /// <param name="cancellationIoCache"></param>
        /// <returns></returns>
        public async Task<DisposableList<ClassBlockObject>> GetIoListBlockDataFromListBlockHeight(HashSet<long> listBlockHeight, bool keepAlive, bool clone, CancellationTokenSource cancellationIoCache)
        {

            DisposableList<ClassBlockObject> listBlockObjectDisposable = new DisposableList<ClassBlockObject>();

            bool useSemaphore = false;

            try
            {
                useSemaphore = await _ioSemaphoreAccess.TryWaitAsync(cancellationIoCache);

                if (!useSemaphore)
                    return listBlockObjectDisposable;

                foreach (long blockHeight in listBlockHeight.ToArray())
                {
                    if (!listBlockHeight.Contains(blockHeight) || _ioStructureObjectsDictionary[blockHeight].IsNull)
                        continue;

                    listBlockObjectDisposable.Add(clone ? _ioStructureObjectsDictionary[blockHeight].BlockObject.DirectCloneBlockObject() : _ioStructureObjectsDictionary[blockHeight].BlockObject);
                    listBlockHeight.Remove(blockHeight);
                }


                if (listBlockHeight.Count > 0)
                {
                    foreach (ClassBlockObject blockObject in await ReadAndGetIoBlockDataFromListBlockHeightOnIoStreamFile(listBlockHeight, cancellationIoCache))
                    {
                        long blockHeight = blockObject.BlockHeight;

                        if (listBlockHeight.Contains(blockHeight))
                        {
                            if (!_ioStructureObjectsDictionary[blockHeight].IsDeleted)
                            {
                                if (_ioStructureObjectsDictionary[blockHeight].IsNull)
                                {
                                    listBlockObjectDisposable.Add(blockObject);
                                    listBlockHeight.Remove(blockHeight);

                                    // Update the io cache file and remove the data updated from the active memory
                                    if (keepAlive)
                                        await InsertInActiveMemory(blockObject, keepAlive, true, cancellationIoCache);
                                }
                                else
                                {
                                    listBlockObjectDisposable.Add(clone ? _ioStructureObjectsDictionary[blockHeight].BlockObject.DirectCloneBlockObject() : _ioStructureObjectsDictionary[blockHeight].BlockObject);
                                    listBlockHeight.Remove(blockHeight);
                                }
                            }
                        }
                    }
                }

            }
            finally
            {
                if (useSemaphore)
                    _ioSemaphoreAccess.Release();
            }

            return listBlockObjectDisposable;
        }

        /// <summary>
        /// Retrieve back a block transaction from a block height target by a transaction index selected.
        /// </summary>
        /// <param name="blockHeight"></param>
        /// <param name="transactionHash"></param>
        /// <param name="cancellationIoCache"></param>
        /// <returns></returns>
        public async Task<ClassBlockTransaction> GetBlockTransactionFromIoBlockHeightByTransactionHash(long blockHeight, string transactionHash, bool keepAlive, CancellationTokenSource cancellationIoCache)
        {
            bool useSemaphore = false;

            try
            {
                useSemaphore = await _ioSemaphoreAccess.TryWaitAsync(cancellationIoCache);

                if (!useSemaphore || !_ioStructureObjectsDictionary.ContainsKey(blockHeight))
                    return null;

                if (_ioStructureObjectsDictionary[blockHeight].IsNull)
                {
                    ClassBlockObject blockObject = await CallGetRetrieveDataAccess(blockHeight, keepAlive, false, false, cancellationIoCache);

                    if (blockObject != null)
                    {
                        if (blockObject.BlockTransactions.ContainsKey(transactionHash))
                            return blockObject.BlockTransactions[transactionHash].Clone();
                    }
                }
                else
                {
                    if (_ioStructureObjectsDictionary[blockHeight].BlockObject.BlockTransactions.ContainsKey(transactionHash))
                        return _ioStructureObjectsDictionary[blockHeight].BlockObject.BlockTransactions[transactionHash].Clone();
                }

            }
            finally
            {
                if (useSemaphore)
                    _ioSemaphoreAccess.Release();
            }

            return null;
        }

        #endregion

        #region Set IO Data functions.

        /// <summary>
        /// Push or update a io block data to the io cache.
        /// </summary>
        /// <param name="blockObject"></param>
        /// <param name="keepAlive"></param>
        /// <param name="cancellationIoCache"></param>
        /// <returns></returns>
        public async Task<bool> PushOrUpdateIoBlockData(ClassBlockObject blockObject, bool keepAlive, CancellationTokenSource cancellationIoCache)
        {
            bool useSemaphore = false;

            try
            {
                useSemaphore = await _ioSemaphoreAccess.TryWaitAsync(cancellationIoCache);

                if (!useSemaphore || blockObject == null)
                    return false;

                if (_ioStructureObjectsDictionary.ContainsKey(blockObject.BlockHeight))
                {
                    blockObject.BlockIsUpdated = true;
                    await InsertInActiveMemory(blockObject, keepAlive, false, cancellationIoCache);
                    _ioStructureObjectsDictionary[blockObject.BlockHeight].IsDeleted = false;
                    return true;
                }
                else
                {
                    try
                    {
                        _ioStructureObjectsDictionary.Add(blockObject.BlockHeight, new ClassCacheIoStructureObject()
                        {
                            IsWritten = false,
                            IoDataPosition = 0
                        });
                    }
                    catch
                    {
                        return false;
                    }

                    if (await WriteNewIoDataOnIoStreamFile(blockObject, cancellationIoCache))
                        return true;
                }
            }
            finally
            {
                if (useSemaphore)
                    _ioSemaphoreAccess.Release();
            }
            return false;
        }

        /// <summary>
        /// Push or update transaction on a io block data.
        /// </summary>
        /// <param name="blockTransaction"></param>
        /// <param name="blockHeight"></param>
        /// <param name="keepAlive"></param>
        /// <param name="cancellationIoCache"></param>
        /// <returns></returns>
        public async Task<bool> PushOrUpdateTransactionOnIoBlockData(ClassBlockTransaction blockTransaction, long blockHeight, bool keepAlive, CancellationTokenSource cancellationIoCache)
        {
            bool useSemaphore = false;

            try
            {
                useSemaphore = await _ioSemaphoreAccess.TryWaitAsync(cancellationIoCache);

                if (!useSemaphore || !_ioStructureObjectsDictionary.ContainsKey(blockHeight))
                    return false;

                if (!_ioStructureObjectsDictionary[blockHeight].IsNull)
                {
                    if (_ioStructureObjectsDictionary[blockHeight].BlockObject.BlockTransactions != null)
                    {
                        if (_ioStructureObjectsDictionary[blockHeight].BlockObject.BlockTransactions.ContainsKey(blockTransaction.TransactionObject.TransactionHash))
                            _ioStructureObjectsDictionary[blockHeight].BlockObject.BlockTransactions[blockTransaction.TransactionObject.TransactionHash] = blockTransaction;

                        else
                            _ioStructureObjectsDictionary[blockHeight].BlockObject.BlockTransactions.Add(blockTransaction.TransactionObject.TransactionHash, blockTransaction);

                        _ioStructureObjectsDictionary[blockHeight].BlockObject.BlockIsUpdated = true;
                        _ioStructureObjectsDictionary[blockHeight].IsDeleted = false;
                        return false;
                    }
                }
                else
                {
                    ClassBlockObject blockObject = await CallGetRetrieveDataAccess(blockHeight, false, false, false, cancellationIoCache);

                    if (blockObject?.BlockTransactions != null)
                    {

                        if (blockObject.BlockTransactions.ContainsKey(blockTransaction.TransactionObject.TransactionHash))
                            blockObject.BlockTransactions[blockTransaction.TransactionObject.TransactionHash] = blockTransaction;
                        else
                            blockObject.BlockTransactions.Add(blockTransaction.TransactionObject.TransactionHash, blockTransaction);

                        blockObject.BlockIsUpdated = true;

                        _ioStructureObjectsDictionary[blockHeight].IsDeleted = false;

                        // Update the io cache file and remove the data updated from the active memory
                        await InsertInActiveMemory(blockObject, true, keepAlive, cancellationIoCache);

                        return true;
                    }
                }

            }
            finally
            {
                if (useSemaphore)
                    _ioSemaphoreAccess.Release();
            }

            return false;
        }

        /// <summary>
        /// Push or update list of io block data to the io cache.
        /// </summary>
        /// <param name="listBlockObject"></param>
        /// <param name="keepAlive"></param>
        /// <param name="cancellationIoCache"></param>
        /// <returns></returns>
        public async Task<bool> PushOrUpdateListIoBlockData(List<ClassBlockObject> listBlockObject, bool keepAlive, CancellationTokenSource cancellationIoCache)
        {
            bool useSemaphore = false;

            try
            {
                useSemaphore = await _ioSemaphoreAccess.TryWaitAsync(cancellationIoCache);

                if (!useSemaphore || listBlockObject.Count == 0)
                    return false;

                int countPushed = 0;

                for (int i = 0; i < listBlockObject.Count; i++)
                {
                    if (cancellationIoCache.IsCancellationRequested)
                        break;

                    if (i < listBlockObject.Count)
                    {
                        if (_ioStructureObjectsDictionary.ContainsKey(listBlockObject[i].BlockHeight))
                        {
                            await InsertInActiveMemory(listBlockObject[i], keepAlive, false, cancellationIoCache);
                            _ioStructureObjectsDictionary[listBlockObject[i].BlockHeight].IsDeleted = false;

                            countPushed++;
                        }
                        else
                        {

                            try
                            {
                                _ioStructureObjectsDictionary.Add(listBlockObject[i].BlockHeight, new ClassCacheIoStructureObject()
                                {
                                    BlockObject = null,
                                    IsWritten = false,
                                    IoDataPosition = 0
                                });
                            }
                            catch
                            {
                                break;
                            }

                            if (await WriteNewIoDataOnIoStreamFile(listBlockObject[i], cancellationIoCache))
                                countPushed++;
                        }


                    }
                }

                return countPushed == listBlockObject.Count;
            }
            finally
            {
                if (useSemaphore)
                    _ioSemaphoreAccess.Release();
            }
        }

        /// <summary>
        /// Push or update list of io block transaction data to the io cache.
        /// </summary>
        /// <param name="keepAlive"></param>
        /// <param name="cancellationIoCache"></param>
        /// <param name="listBlockTransaction"></param>
        /// <returns></returns>
        public async Task<bool> PushOrUpdateListIoBlockTransactionData(List<ClassBlockTransaction> listBlockTransaction, bool keepAlive, CancellationTokenSource cancellationIoCache)
        {

            bool useSemaphore = false;
            int countPushed = 0;

            try
            {
                useSemaphore = await _ioSemaphoreAccess.TryWaitAsync(cancellationIoCache);

                if (!useSemaphore || listBlockTransaction.Count == 0)
                    return false;


                for (int i = 0; i < listBlockTransaction.Count; i++)
                {
                    if (cancellationIoCache.IsCancellationRequested)
                        break;

                    if (i < listBlockTransaction.Count)
                    {
                        long blockHeight = listBlockTransaction[i].TransactionObject.BlockHeightTransaction;
                        string transactionHash = listBlockTransaction[i].TransactionObject.TransactionHash;

                        if (_ioStructureObjectsDictionary.ContainsKey(blockHeight))
                        {
                            if (!_ioStructureObjectsDictionary[blockHeight].IsNull)
                            {
                                if (_ioStructureObjectsDictionary[blockHeight].BlockObject.BlockTransactions != null)
                                {
                                    if (_ioStructureObjectsDictionary[blockHeight].BlockObject.BlockTransactions.ContainsKey(transactionHash))
                                        _ioStructureObjectsDictionary[blockHeight].BlockObject.BlockTransactions[transactionHash] = listBlockTransaction[i];
                                    else
                                        _ioStructureObjectsDictionary[blockHeight].BlockObject.BlockTransactions.Add(transactionHash, listBlockTransaction[i]);

                                    _ioStructureObjectsDictionary[blockHeight].BlockObject.BlockIsUpdated = true;
                                    _ioStructureObjectsDictionary[blockHeight].BlockObject.BlockLastChangeTimestamp = TaskManager.TaskManager.CurrentTimestampMillisecond;
                                    countPushed++;
                                }
                            }
                            else
                            {

                                ClassBlockObject blockObject = await CallGetRetrieveDataAccess(blockHeight, keepAlive, false, false, cancellationIoCache);

                                if (blockObject?.BlockTransactions != null)
                                {
                                    if (blockObject.BlockTransactions.ContainsKey(transactionHash))
                                        blockObject.BlockTransactions[transactionHash] = listBlockTransaction[i];
                                    else
                                        blockObject.BlockTransactions.Add(transactionHash, listBlockTransaction[i]);

                                    blockObject.BlockIsUpdated = true;
                                    blockObject.BlockLastChangeTimestamp = TaskManager.TaskManager.CurrentTimestampMillisecond;

                                    // Update the io cache file and remove the data updated from the active memory
                                    await InsertInActiveMemory(blockObject, keepAlive, false, cancellationIoCache);
                                    countPushed++;
                                }
                            }
                        }
                    }
                }

                return countPushed == listBlockTransaction.Count;
            }
            finally
            {
                if (useSemaphore)
                    _ioSemaphoreAccess.Release();
            }
        }

        #endregion

        #region Manage content functions.

        /// <summary>
        /// Delete a io block data, it's done later after a purge of data or by another set.
        /// </summary>
        /// <param name="blockHeight"></param>
        /// <param name="cancellationIoCache"></param>
        /// <returns></returns>
        public async Task<bool> TryDeleteIoBlockData(long blockHeight, CancellationTokenSource cancellationIoCache)
        {
            bool useSemaphore = false;

            try
            {
                useSemaphore = await _ioSemaphoreAccess.TryWaitAsync(cancellationIoCache);

                if (!useSemaphore || !_ioStructureObjectsDictionary.ContainsKey(blockHeight))
                    return false;

                _ioStructureObjectsDictionary[blockHeight].IsDeleted = true;
                _ioStructureObjectsDictionary[blockHeight].BlockObject = null;
                return true;
            }
            finally
            {
                if (useSemaphore)
                    _ioSemaphoreAccess.Release();
            }
        }

        /// <summary>
        /// Check if the io cache contain a block height target.
        /// </summary>
        /// <param name="blockHeight"></param>
        /// <param name="cancellation"></param>
        /// <returns></returns>
        public async Task<bool> ContainsIoBlockHeight(long blockHeight, CancellationTokenSource cancellation)
        {
            bool useSemaphore = false;

            try
            {
                useSemaphore = await _ioSemaphoreAccess.TryWaitAsync(cancellation);

                if (!useSemaphore)
                    return false;

                return _ioStructureObjectsDictionary.ContainsKey(blockHeight);

            }
            finally
            {
                if (useSemaphore)
                    _ioSemaphoreAccess.Release();
            }
        }

        /// <summary>
        /// Check if io blocks contain a transaction hash.
        /// </summary>
        /// <param name="transactionHash"></param>
        /// <param name="blockHeight"></param>
        /// <param name="cancellationIoCache"></param>
        /// <returns></returns>
        public async Task<bool> ContainIoBlockTransactionHash(string transactionHash, long blockHeight, CancellationTokenSource cancellationIoCache)
        {

            bool useSemaphore = false;

            try
            {
                useSemaphore = await _ioSemaphoreAccess.TryWaitAsync(cancellationIoCache);

                if (!useSemaphore || blockHeight <= BlockchainSetting.GenesisBlockHeight || !_ioStructureObjectsDictionary.ContainsKey(blockHeight))
                    return false;

                if (!_ioStructureObjectsDictionary[blockHeight].IsNull)
                {
                    if (_ioStructureObjectsDictionary[blockHeight].BlockObject.BlockTransactions.ContainsKey(transactionHash))
                        return true;
                }
                else
                {
                    ClassBlockObject blockObject = await CallGetRetrieveDataAccess(blockHeight, false, false, false, cancellationIoCache);

                    if (blockObject != null)
                    {
                        if (blockObject.BlockTransactions.ContainsKey(transactionHash))
                            return true;
                    }
                }
            }
            finally
            {
                if (useSemaphore)
                    _ioSemaphoreAccess.Release();
            }

            return false;
        }

        /// <summary>
        /// Return the list of block height indexed.
        /// </summary>
        /// <returns></returns>
        public async Task<HashSet<long>> GetIoBlockHeightListIndexed(CancellationTokenSource cancellation)
        {
            bool useSemaphore = false;

            try
            {
                useSemaphore = await _ioSemaphoreAccess.TryWaitAsync(cancellation);

                if (!useSemaphore)
                    return new HashSet<long>();

                return new HashSet<long>(_ioStructureObjectsDictionary.Keys.ToList());

            }
            finally
            {
                if (useSemaphore)
                    _ioSemaphoreAccess.Release();
            }
        }

        #endregion

        #endregion

        #region Manage IO Data functions.

        /// <summary>
        /// Clean io memory data in the active memory.
        /// </summary>
        /// <returns></returns>
        public async Task<long> PurgeIoBlockDataMemory(bool semaphoreUsed, CancellationTokenSource cancellation, long totalMemoryAsked, bool enableAskMemory)
        {
            bool useSemaphore = false;
            long totalAvailableMemoryRetrieved = 0;

            try
            {
                if (!semaphoreUsed)
                {
                    if (cancellation == null)
                        await _ioSemaphoreAccess.WaitAsync().ConfigureAwait(false);
                    else
                        await _ioSemaphoreAccess.WaitAsync(cancellation.Token).ConfigureAwait(false);
                    useSemaphore = true;
                }

                using (DisposableDictionary<long, ClassBlockObject> listIoData = new DisposableDictionary<long, ClassBlockObject>())
                {
                    HashSet<long> listIoDataDeleted = new HashSet<long>();

                    #region Sort data to cache, sort data to keep in active memory and sort data to delete forever.

                    long timestamp = TaskManager.TaskManager.CurrentTimestampMillisecond;

                    foreach (long ioBlockHeight in _ioStructureObjectsDictionary.Keys)
                    {


                        if (!_ioStructureObjectsDictionary[ioBlockHeight].IsDeleted)
                        {
                            if (!_ioStructureObjectsDictionary[ioBlockHeight].IsNull)
                            {
                                // List updated blocks to remove of the memory allocated to the cache system.
                                if (_ioStructureObjectsDictionary[ioBlockHeight].IsUpdated)
                                {
                                    bool getOutBlock =
                                        (_ioStructureObjectsDictionary[ioBlockHeight].LastUpdateTimestamp + _blockchainDatabaseSetting.BlockchainCacheSetting.IoCacheDiskMaxKeepAliveDataInMemoryTimeLimit < timestamp
                                        &&
                                        _ioStructureObjectsDictionary[ioBlockHeight].LastGetTimestamp + _blockchainDatabaseSetting.BlockchainCacheSetting.IoCacheDiskMaxKeepAliveDataInMemoryTimeLimit < timestamp)
                                        || enableAskMemory;

                                    if (getOutBlock)
                                    {
                                        if (enableAskMemory)
                                            totalAvailableMemoryRetrieved += _ioStructureObjectsDictionary[ioBlockHeight].IoDataSizeOnMemory;

                                        listIoData.Add(ioBlockHeight, _ioStructureObjectsDictionary[ioBlockHeight].BlockObject);
                                    }
                                }
                                // Directly clean up block(s) not updated.
                                else
                                {
                                    if (enableAskMemory)
                                        totalAvailableMemoryRetrieved += _ioStructureObjectsDictionary[ioBlockHeight].IoDataSizeOnMemory;

                                    _ioStructureObjectsDictionary[ioBlockHeight].BlockObject = null;
                                }
                            }
                        }
                        else
                        {
                            if (!listIoDataDeleted.Contains(ioBlockHeight))
                                listIoDataDeleted.Add(ioBlockHeight);
                        }

                        if (enableAskMemory)
                        {
                            if (totalMemoryAsked <= totalAvailableMemoryRetrieved)
                                break;
                        }
                    }

                    #endregion

                    if (enableAskMemory && totalMemoryAsked > totalAvailableMemoryRetrieved)
                    {
                        if (_ioCacheSystem.GetIoCacheSystemMemoryConsumption(cancellation, out _) +
                            (totalMemoryAsked - totalAvailableMemoryRetrieved) <= _blockchainDatabaseSetting.BlockchainCacheSetting.GlobalMaxActiveMemoryAllocationFromCache)
                            totalAvailableMemoryRetrieved = totalMemoryAsked;
                    }

                    #region Update the io cache file.

                    // Deleted records must be physically removed before their in-memory
                    // index entries are discarded, otherwise a restart could resurrect them.
                    bool fullPurge = listIoDataDeleted.Count > 0 ||
                        (_totalIoCacheFileSize > 0 && _lastIoCacheTotalFileSizeWritten > 0
                            ? ((double)_lastIoCacheTotalFileSizeWritten / _totalIoCacheFileSize) * 100d >= _blockchainDatabaseSetting.BlockchainCacheSetting.IoCacheDiskFullPurgeEnablePercentWrite
                            : false);

                    bool cacheFileUpdated = true;
                    if (fullPurge || listIoData.Count > 0)
                        cacheFileUpdated = await ReplaceIoDataListOnIoStreamFile(listIoData.GetList, cancellation, fullPurge).ConfigureAwait(false);

                    if (cacheFileUpdated)
                    {
                        foreach (long blockHeight in listIoData.GetList.Keys.ToArray())
                        {
                            if (_ioStructureObjectsDictionary.ContainsKey(blockHeight) &&
                                !_ioStructureObjectsDictionary[blockHeight].IsNull)
                                _ioStructureObjectsDictionary[blockHeight].BlockObject = null;
                        }

                        if (fullPurge)
                        {
                            _lastIoCacheTotalFileSizeWritten = 0;
                            ClassUtility.CleanGc();
                        }
                    }

                    #endregion

                    #region Delete listed io cache data to delete forever.

                    if (cacheFileUpdated && listIoDataDeleted.Count > 0)
                    {
                        foreach (var ioBlockHeight in listIoDataDeleted)
                        {
                            if (_ioStructureObjectsDictionary.ContainsKey(ioBlockHeight))
                            {
                                if (!_ioStructureObjectsDictionary[ioBlockHeight].IsNull)
                                    _ioStructureObjectsDictionary[ioBlockHeight].BlockObject = null;

                                _ioStructureObjectsDictionary.Remove(ioBlockHeight);
                            }
                        }

                        listIoDataDeleted.Clear();
                    }

                    #endregion

                }
            }
            finally
            {

                if (useSemaphore)
                    _ioSemaphoreAccess.Release();

            }

            return totalAvailableMemoryRetrieved;
        }

        /// <summary>
        /// Total Memory usage.
        /// </summary>
        /// <param name="blockObject"></param>
        /// <param name="fromReading"></param>
        /// <param name="cancellationIoCache"></param>
        /// <param name="keepAlive"></param>
        /// <returns></returns>
        private async Task InsertInActiveMemory(ClassBlockObject blockObject, bool keepAlive, bool fromReading, CancellationTokenSource cancellationIoCache)
        {
            bool insertInMemory = false;
            bool cancel = blockObject == null;

            if (!cancel)
            {
                if (keepAlive)
                {
                    if (!_ioStructureObjectsDictionary[blockObject.BlockHeight].IsNull)
                    {
                        _ioStructureObjectsDictionary[blockObject.BlockHeight].BlockObject = blockObject;
                        insertInMemory = true;
                    }
                    else
                    {
                        long blockMemorySize = _ioStructureObjectsDictionary[blockObject.BlockHeight].IoDataSizeOnMemory == 0 ? ClassBlockUtility.GetIoBlockSizeOnMemory(blockObject) : _ioStructureObjectsDictionary[blockObject.BlockHeight].IoDataSizeOnMemory;

                        _ioStructureObjectsDictionary[blockObject.BlockHeight].IoDataSizeOnMemory = blockMemorySize;

                        if (blockMemorySize <= _blockchainDatabaseSetting.BlockchainCacheSetting.GlobalMaxActiveMemoryAllocationFromCache)
                        {

                            long totalMemoryConsumption = _ioCacheSystem.GetIoCacheSystemMemoryConsumption(cancellationIoCache, out _);

                            // Try to push the block updated in memory.
                            if (totalMemoryConsumption + blockMemorySize <= _blockchainDatabaseSetting.BlockchainCacheSetting.GlobalMaxActiveMemoryAllocationFromCache)
                            {
                                _ioStructureObjectsDictionary[blockObject.BlockHeight].BlockObject = blockObject;
                                _ioStructureObjectsDictionary[blockObject.BlockHeight].IoDataSizeOnMemory = blockMemorySize;
                                insertInMemory = true;
                            }
                            else
                            {
                                // Do an internal purge of the active memory on this io cache file.
                                long totalMemoryRetrieved = await PurgeIoBlockDataMemory(true, cancellationIoCache, blockMemorySize, true);

                                if (totalMemoryRetrieved >= blockMemorySize)
                                {
                                    _ioStructureObjectsDictionary[blockObject.BlockHeight].BlockObject = blockObject;
                                    _ioStructureObjectsDictionary[blockObject.BlockHeight].IoDataSizeOnMemory = blockMemorySize;
                                    insertInMemory = true;
                                }
                                else if (_ioCacheSystem.GetIoCacheSystemMemoryConsumption(cancellationIoCache, out _) + (blockMemorySize - totalMemoryRetrieved) <= _blockchainDatabaseSetting.BlockchainCacheSetting.GlobalMaxActiveMemoryAllocationFromCache)
                                {
                                    _ioStructureObjectsDictionary[blockObject.BlockHeight].BlockObject = blockObject;
                                    _ioStructureObjectsDictionary[blockObject.BlockHeight].IoDataSizeOnMemory = blockMemorySize;
                                    insertInMemory = true;
                                }
                                else
                                {
                                    // Do a purge on other io cache files indexed.
                                    if (await _ioCacheSystem.DoPurgeFromIoCacheIndex(_ioDataStructureFilename, blockMemorySize, cancellationIoCache))
                                    {
                                        _ioStructureObjectsDictionary[blockObject.BlockHeight].BlockObject = blockObject;
                                        _ioStructureObjectsDictionary[blockObject.BlockHeight].IoDataSizeOnMemory = blockMemorySize;
                                        insertInMemory = true;
                                    }
                                }
                            }
                        }
                    }
                }

                if (!insertInMemory && !fromReading)
                {
                    if (!_ioStructureObjectsDictionary[blockObject.BlockHeight].IsNull)
                        _ioStructureObjectsDictionary[blockObject.BlockHeight].BlockObject = blockObject;
                    else
                    {
                        if (blockObject.BlockIsUpdated)
                            await WriteNewIoDataOnIoStreamFile(blockObject, cancellationIoCache);
                    }
                }
            }
        }

        /// <summary>
        /// Return the memory usage of active memory from the io cache file index.
        /// </summary>
        /// <returns></returns>
        public long GetIoMemoryUsage(CancellationTokenSource cancellation, out int totalBlockKeepAlive)
        {
            long memoryUsage = 0;
            totalBlockKeepAlive = 0;

            foreach (long blockHeight in _ioStructureObjectsDictionary.Keys.ToArray())
            {
                if (_ioStructureObjectsDictionary[blockHeight].IsNull)
                    continue;

                memoryUsage += _ioStructureObjectsDictionary[blockHeight].IoDataSizeOnMemory;
                totalBlockKeepAlive++;
            }

            return memoryUsage;
        }

        #endregion

        #region Read IO Data functions.

        /// <summary>
        /// Retrieve back a io block data target by a block height cached on a io stream file.
        /// </summary>
        /// <param name="blockHeight"></param>
        /// <param name="cancellation"></param>
        /// <param name="blockInformationsOnly"></param>
        /// <returns></returns>
        private async Task<ClassBlockObject> ReadAndGetIoBlockDataFromBlockHeightOnIoStreamFile(long blockHeight, CancellationTokenSource cancellation, bool blockInformationsOnly)
        {

            await WaitIoWriteDone(cancellation);

            // The cache stream can be closed during a cache-file replacement.
            // Reopen it before seeking/reading so a previous interrupted replacement
            // cannot leave subsequent block-statistics reads using a closed stream.
            OpenLockStream();

            ClassBlockObject blockObject = null;

            // If the data is indicated to be inside the io cache file.
            if (_ioStructureObjectsDictionary[blockHeight].IsWritten)
            {
                long ioDataPosition = _ioStructureObjectsDictionary[blockHeight].IoDataPosition;
                long ioDataSize = _ioStructureObjectsDictionary[blockHeight].IoDataSizeOnFile;

                // Read by seek and position registered directly.
                _ioDataStructureFileLockStream.Position = ioDataPosition;

                Tuple<bool, byte[]> readResult = await ReadByBlock(ioDataSize, cancellation, _ioDataStructureFileLockStream);

                if (readResult.Item1)
                {
                    string ioDataLine = _ioDataUtf8Encoding.GetString(readResult.Item2);

                    if (BlockHeightMatchToIoDataLine(blockHeight, ioDataLine))
                        IoStringDataLineToBlockObject(ioDataLine, _blockchainDatabaseSetting, blockInformationsOnly, cancellation, out blockObject);
                }
            }

            return blockObject;
        }

        /// <summary>
        /// Retrieve back a io block data target by a block height cached on a io stream file.
        /// </summary>
        /// <param name="listBlockHeight"></param>
        /// <param name="cancellation"></param>
        /// <returns></returns>
        private async Task<List<ClassBlockObject>> ReadAndGetIoBlockDataFromListBlockHeightOnIoStreamFile(HashSet<long> listBlockHeight, CancellationTokenSource cancellation)
        {
            await WaitIoWriteDone(cancellation);

            // Ensure the shared cache stream is open before any Position/Read operation.
            OpenLockStream();

            List<ClassBlockObject> listBlockObject = new List<ClassBlockObject>();

            HashSet<long> listBlockHeightFound = new HashSet<long>();

            // Direct research.
            foreach (long blockHeight in listBlockHeight)
            {


                if (!_ioStructureObjectsDictionary[blockHeight].IsDeleted)
                {
                    if (_ioStructureObjectsDictionary[blockHeight].IsNull)
                    {
                        // If the data is indicated to be inside the io cache file.
                        if (_ioStructureObjectsDictionary[blockHeight].IsWritten)
                        {

                            // Read by seek and position registered directly.
                            long ioDataPosition = _ioStructureObjectsDictionary[blockHeight].IoDataPosition;
                            long ioDataSize = _ioStructureObjectsDictionary[blockHeight].IoDataSizeOnFile;

                            // Read by seek and position registered directly.
                            _ioDataStructureFileLockStream.Position = ioDataPosition;

                            var readResult = await ReadByBlock(ioDataSize, cancellation, _ioDataStructureFileLockStream);

                            if (readResult.Item1)
                            {
                                var ioDataLine = _ioDataUtf8Encoding.GetString(readResult.Item2);

                                if (BlockHeightMatchToIoDataLine(blockHeight, ioDataLine))
                                {
                                    if (IoStringDataLineToBlockObject(ioDataLine, _blockchainDatabaseSetting, false, cancellation, out ClassBlockObject blockObject))
                                    {
                                        listBlockHeightFound.Add(blockHeight);
                                        listBlockObject.Add(blockObject);
                                    }
                                }

                                // Clean up.
                                ioDataLine.Clear();
                            }
                            else
                                break;
                        }
                    }
                    else
                    {
                        listBlockObject.Add(_ioStructureObjectsDictionary[blockHeight].BlockObject);
                        listBlockHeightFound.Add(blockHeight);
                    }
                }
            }

            if (listBlockHeightFound.Count < listBlockHeight.Count)
            {
                int blockLeft = listBlockHeight.Count - listBlockHeightFound.Count;
                ClassLog.WriteLine("Cache IO Index Object - Retrieve back " + listBlockHeight.Count + " failed. " + blockLeft + " block(s) not retrieved.", ClassEnumLogLevelType.LOG_LEVEL_MEMORY_MANAGER, ClassEnumLogWriteLevel.LOG_WRITE_LEVEL_MANDATORY_PRIORITY, false, ConsoleColor.DarkRed);
            }

            // Clean up.
            listBlockHeightFound.Clear();

            return listBlockObject;
        }

        /// <summary>
        /// Read data by block.
        /// </summary>
        /// <param name="ioFileSize"></param>
        /// <param name="cancellation"></param>
        /// <param name="ioFileStream"></param>
        /// <returns></returns>
        private async Task<Tuple<bool, byte[]>> ReadByBlock(long ioFileSize, CancellationTokenSource cancellation, FileStream ioFileStream)
        {
            // A byte[] is indexed by Int32. Reject invalid sizes before allocating.
            if (ioFileStream == null || ioFileSize < 0 || ioFileSize > int.MaxValue || !ioFileStream.CanRead)
                return new Tuple<bool, byte[]>(false, Array.Empty<byte>());

            byte[] data = new byte[(int)ioFileSize];
            int totalRead = 0;

            try
            {
                while (totalRead < data.Length)
                {
                    if (cancellation != null)
                        cancellation.Token.ThrowIfCancellationRequested();

                    int read = cancellation == null
                        ? await ioFileStream.ReadAsync(data, totalRead, data.Length - totalRead).ConfigureAwait(false)
                        : await ioFileStream.ReadAsync(data, totalRead, data.Length - totalRead, cancellation.Token).ConfigureAwait(false);

                    // Stream.ReadAsync is allowed to return fewer bytes than requested.
                    // A zero result before the expected length means EOF/truncation.
                    if (read <= 0)
                        return new Tuple<bool, byte[]>(false, data.Take(totalRead).ToArray());

                    totalRead += read;
                }

                return new Tuple<bool, byte[]>(true, data);
            }
            catch (OperationCanceledException)
            {
                return new Tuple<bool, byte[]>(false, data.Take(totalRead).ToArray());
            }
            catch (IOException)
            {
                return new Tuple<bool, byte[]>(false, data.Take(totalRead).ToArray());
            }
            catch (ObjectDisposedException)
            {
                return new Tuple<bool, byte[]>(false, data.Take(totalRead).ToArray());
            }
            catch (Exception)
            {
                return new Tuple<bool, byte[]>(false, data.Take(totalRead).ToArray());
            }
        }

        #endregion

        #region Write IO Data functions.

        /// <summary>
        /// Write directly a io data on the io cache file.
        /// </summary>
        /// <param name="blockObject"></param>
        /// <param name="cancellation"></param>
        /// <returns></returns>
        private async Task<bool> WriteNewIoDataOnIoStreamFile(ClassBlockObject blockObject, CancellationTokenSource cancellation)
        {
            if (blockObject == null || _ioDataStructureFileLockStream == null || !_ioDataStructureFileLockStream.CanWrite)
                return false;

            await WaitIoWriteDone(cancellation).ConfigureAwait(false);
            _ioOnWriteData = true;

            long position = _lastIoCacheFileLength;
            bool committed = false;

            try
            {
                if (cancellation != null)
                    cancellation.Token.ThrowIfCancellationRequested();

                if (!_ioStructureObjectsDictionary.ContainsKey(blockObject.BlockHeight))
                    return false;

                // Only append at the last committed offset. Refuse a file shorter than
                // the index, and remove any orphan tail left by a previous interrupted write.
                if (_ioDataStructureFileLockStream.Length < position)
                    return false;

                if (_ioDataStructureFileLockStream.Length > position)
                {
                    _ioDataStructureFileLockStream.SetLength(position);
                    await _ioDataStructureFileLockStream.FlushAsync().ConfigureAwait(false);
                }

                _ioDataStructureFileLockStream.Position = position;
                long dataLength = 0;

                foreach (string ioDataLine in BlockObjectToIoStringData(blockObject, _blockchainDatabaseSetting, cancellation))
                {
                    if (cancellation != null)
                        cancellation.Token.ThrowIfCancellationRequested();

                    byte[] lineBytes = _ioDataUtf8Encoding.GetBytes(ioDataLine);
                    Tuple<bool, long> writeResult = await WriteByBlock(lineBytes, cancellation, _ioDataStructureFileLockStream).ConfigureAwait(false);
                    if (!writeResult.Item1 || writeResult.Item2 != lineBytes.Length)
                        throw new IOException("Incomplete cache I/O write.");

                    dataLength += writeResult.Item2;
                }

                _ioDataStructureFileLockStream.WriteByte((byte)'\r');
                _ioDataStructureFileLockStream.WriteByte((byte)'\n');

                if (cancellation != null)
                    cancellation.Token.ThrowIfCancellationRequested();

                if (cancellation == null)
                    await _ioDataStructureFileLockStream.FlushAsync().ConfigureAwait(false);
                else
                    await _ioDataStructureFileLockStream.FlushAsync(cancellation.Token).ConfigureAwait(false);

                // Commit in-memory metadata only after the complete record has been flushed.
                ClassCacheIoStructureObject structure = _ioStructureObjectsDictionary[blockObject.BlockHeight];
                long previousSize = structure.IsWritten ? structure.IoDataSizeOnFile : 0;
                _totalIoCacheFileSize = Math.Max(0, _totalIoCacheFileSize - previousSize) + dataLength;
                structure.IsWritten = true;
                structure.IoDataSizeOnFile = dataLength;
                structure.IoDataPosition = position;
                _lastIoCacheTotalFileSizeWritten += dataLength;
                _lastIoCacheFileLength = position + dataLength + 2;
                committed = true;
                return true;
            }
            catch (OperationCanceledException)
            {
                return false;
            }
            catch (IOException)
            {
                return false;
            }
            catch (ObjectDisposedException)
            {
                return false;
            }
            catch (Exception)
            {
                return false;
            }
            finally
            {
                if (!committed && _ioDataStructureFileLockStream != null)
                {
                    try
                    {
                        // Roll back only when the stream still reaches the committed boundary.
                        // Never extend a truncated/corrupt file with zero-filled bytes.
                        if (_ioDataStructureFileLockStream.Length >= position)
                        {
                            _ioDataStructureFileLockStream.SetLength(position);
                            _ioDataStructureFileLockStream.Position = position;
                        }
                    }
                    catch
                    {
                        // Keep the cache marked as needing recovery by not advancing its committed length.
                    }
                }

                _ioOnWriteData = false;
            }
        }

        /// <summary>
        /// Replace or append IO cache records safely.
        /// The in-memory index is changed only after the corresponding disk operation
        /// has completed successfully. A full purge uses a same-directory temporary
        /// file and File.Replace so the previous cache remains available on failure.
        /// </summary>
        private async Task<bool> ReplaceIoDataListOnIoStreamFile(
            Dictionary<long, ClassBlockObject> blockListObject,
            CancellationTokenSource cancellation,
            bool fullPurge)
        {
            if (blockListObject == null)
                return false;

            await WaitIoWriteDone(cancellation).ConfigureAwait(false);
            _ioOnWriteData = true;

            string temporaryPath = _ioDataStructureFilePath + ".tmp";
            string backupPath = _ioDataStructureFilePath + ".bak." + Guid.NewGuid().ToString("N");
            bool replacementCommitted = false;
            bool appendCommitted = false;

            try
            {
                if (cancellation != null)
                    cancellation.Token.ThrowIfCancellationRequested();

                if (_ioStreamsClosed || _ioDataStructureFileLockStream == null)
                    OpenLockStream();

                if (fullPurge)
                {
                    // Never overwrite the only valid copy while constructing a purge.
                    if (File.Exists(temporaryPath))
                        File.Delete(temporaryPath);

                    Dictionary<long, Tuple<long, long>> stagedPositions =
                        new Dictionary<long, Tuple<long, long>>();
                    HashSet<long> copiedFromExistingFile = new HashSet<long>();
                    long stagedDataSize = 0;

                    using (FileStream writer = new FileStream(
                        temporaryPath,
                        FileMode.CreateNew,
                        FileAccess.Write,
                        FileShare.None,
                        _blockchainDatabaseSetting.BlockchainCacheSetting.IoCacheDiskWriteStreamBufferSize,
                        FileOptions.Asynchronous | FileOptions.SequentialScan))
                    {
                        // Preserve valid records that are not being replaced by memory data.
                        foreach (long blockHeight in _ioStructureObjectsDictionary.Keys.ToArray())
                        {
                            if (cancellation != null)
                                cancellation.Token.ThrowIfCancellationRequested();

                            if (blockListObject.ContainsKey(blockHeight))
                                continue;

                            ClassCacheIoStructureObject structure = _ioStructureObjectsDictionary[blockHeight];
                            if (structure.IsDeleted || !structure.IsWritten)
                                continue;

                            long sourcePosition = structure.IoDataPosition;
                            long sourceSize = structure.IoDataSizeOnFile;
                            long sourceLength = _ioDataStructureFileLockStream.Length;

                            if (sourcePosition < 0 || sourceSize <= 0 ||
                                sourcePosition > sourceLength || sourceSize > sourceLength - sourcePosition)
                                throw new IOException("Invalid cache record bounds during purge.");

                            _ioDataStructureFileLockStream.Position = sourcePosition;
                            Tuple<bool, byte[]> readResult = await ReadByBlock(
                                sourceSize, cancellation, _ioDataStructureFileLockStream).ConfigureAwait(false);

                            if (!readResult.Item1 || readResult.Item2.LongLength != sourceSize)
                                throw new IOException("Unable to read a complete cache record during purge.");

                            string recordText = _ioDataUtf8Encoding.GetString(readResult.Item2);
                            if (!BlockHeightMatchToIoDataLine(blockHeight, recordText))
                                throw new IOException("Cache record identity validation failed during purge.");

                            long targetPosition = writer.Position;
                            Tuple<bool, long> writeResult = await WriteByBlock(
                                readResult.Item2, cancellation, writer).ConfigureAwait(false);

                            if (!writeResult.Item1 || writeResult.Item2 != sourceSize)
                                throw new IOException("Unable to copy a complete cache record during purge.");

                            writer.WriteByte((byte)'\r');
                            writer.WriteByte((byte)'\n');

                            stagedPositions[blockHeight] = Tuple.Create(targetPosition, sourceSize);
                            copiedFromExistingFile.Add(blockHeight);
                            stagedDataSize += sourceSize;
                        }

                        // Append replacement/new records after preserved records.
                        foreach (long blockHeight in blockListObject.Keys.ToArray())
                        {
                            if (cancellation != null)
                                cancellation.Token.ThrowIfCancellationRequested();

                            ClassBlockObject blockObject;
                            if (!_ioStructureObjectsDictionary.ContainsKey(blockHeight) ||
                                !blockListObject.TryGetValue(blockHeight, out blockObject) ||
                                blockObject == null)
                                throw new IOException("Invalid block supplied for cache purge.");

                            long targetPosition = writer.Position;
                            long recordSize = 0;

                            foreach (string ioDataLine in BlockObjectToIoStringData(
                                blockObject, _blockchainDatabaseSetting, cancellation))
                            {
                                if (cancellation != null)
                                    cancellation.Token.ThrowIfCancellationRequested();

                                byte[] lineBytes = _ioDataUtf8Encoding.GetBytes(ioDataLine);
                                Tuple<bool, long> writeResult = await WriteByBlock(
                                    lineBytes, cancellation, writer).ConfigureAwait(false);

                                if (!writeResult.Item1 || writeResult.Item2 != lineBytes.Length)
                                    throw new IOException("Incomplete replacement record write.");

                                recordSize += writeResult.Item2;
                            }

                            writer.WriteByte((byte)'\r');
                            writer.WriteByte((byte)'\n');
                            stagedPositions[blockHeight] = Tuple.Create(targetPosition, recordSize);
                            stagedDataSize += recordSize;
                        }

                        if (cancellation != null)
                            cancellation.Token.ThrowIfCancellationRequested();

                        await writer.FlushAsync().ConfigureAwait(false);
                        // Flush(true) requests that buffered file data be pushed to the device.
                        // It is synchronous by design and is available on the supported targets.
                        writer.Flush(true);
                    }

                    // Cancellation is honored until the commit point. Once replacement starts,
                    // complete the commit rather than abandoning an ambiguous file state.
                    if (cancellation != null)
                        cancellation.Token.ThrowIfCancellationRequested();

                    CloseLockStream();
                    try
                    {
                        // Source and destination are in the same directory/volume.
                        // Keep a backup until the new stream has been reopened successfully.
                        File.Replace(temporaryPath, _ioDataStructureFilePath, backupPath, true);
                        replacementCommitted = true;
                    }
                    catch
                    {
                        // The original file is still authoritative if File.Replace failed.
                        OpenLockStream();
                        throw;
                    }

                    // Commit metadata only after the disk replacement succeeded.
                    foreach (KeyValuePair<long, ClassCacheIoStructureObject> entry in _ioStructureObjectsDictionary)
                    {
                        if (entry.Value.IsWritten)
                        {
                            entry.Value.IsWritten = false;
                            entry.Value.IoDataPosition = 0;
                            entry.Value.IoDataSizeOnFile = 0;
                        }
                    }

                    foreach (KeyValuePair<long, Tuple<long, long>> entry in stagedPositions)
                    {
                        ClassCacheIoStructureObject structure = _ioStructureObjectsDictionary[entry.Key];
                        structure.IoDataPosition = entry.Value.Item1;
                        structure.IoDataSizeOnFile = entry.Value.Item2;
                        structure.IsWritten = true;
                    }

                    _totalIoCacheFileSize = stagedDataSize;
                    _lastIoCacheFileLength = new FileInfo(_ioDataStructureFilePath).Length;

                    foreach (long blockHeight in copiedFromExistingFile)
                    {
                        ClassCacheIoStructureObject structure = _ioStructureObjectsDictionary[blockHeight];
                        if (!structure.IsNull)
                            structure.BlockObject = null;
                    }

                    foreach (long blockHeight in blockListObject.Keys.ToArray())
                    {
                        ClassCacheIoStructureObject structure = _ioStructureObjectsDictionary[blockHeight];
                        if (!structure.IsNull)
                            structure.BlockObject = null;
                        blockListObject.Remove(blockHeight);
                    }

                    OpenLockStream();
                    if (File.Exists(backupPath))
                        File.Delete(backupPath);

                    return true;
                }

                // Append path: treat the whole batch as one transaction. If any record
                // fails, truncate to the last committed boundary and leave metadata intact.
                long appendStart = _lastIoCacheFileLength;
                if (appendStart < 0 || _ioDataStructureFileLockStream.Length < appendStart)
                    throw new IOException("Cache file is shorter than its committed length.");

                if (_ioDataStructureFileLockStream.Length > appendStart)
                {
                    // Remove an uncommitted tail left by an earlier interrupted write.
                    _ioDataStructureFileLockStream.SetLength(appendStart);
                    await _ioDataStructureFileLockStream.FlushAsync().ConfigureAwait(false);
                }

                Dictionary<long, Tuple<long, long>> appendPositions =
                    new Dictionary<long, Tuple<long, long>>();
                long appendDataSize = 0;
                _ioDataStructureFileLockStream.Position = appendStart;

                foreach (long blockHeight in blockListObject.Keys.ToArray())
                {
                    if (cancellation != null)
                        cancellation.Token.ThrowIfCancellationRequested();

                    ClassBlockObject blockObject;
                    if (!_ioStructureObjectsDictionary.ContainsKey(blockHeight) ||
                        !blockListObject.TryGetValue(blockHeight, out blockObject) ||
                        blockObject == null)
                        throw new IOException("Invalid block supplied for cache append.");

                    long recordPosition = _ioDataStructureFileLockStream.Position;
                    long recordSize = 0;

                    foreach (string ioDataLine in BlockObjectToIoStringData(
                        blockObject, _blockchainDatabaseSetting, cancellation))
                    {
                        if (cancellation != null)
                            cancellation.Token.ThrowIfCancellationRequested();

                        byte[] lineBytes = _ioDataUtf8Encoding.GetBytes(ioDataLine);
                        Tuple<bool, long> writeResult = await WriteByBlock(
                            lineBytes, cancellation, _ioDataStructureFileLockStream).ConfigureAwait(false);

                        if (!writeResult.Item1 || writeResult.Item2 != lineBytes.Length)
                            throw new IOException("Incomplete cache append write.");

                        recordSize += writeResult.Item2;
                    }

                    _ioDataStructureFileLockStream.WriteByte((byte)'\r');
                    _ioDataStructureFileLockStream.WriteByte((byte)'\n');
                    appendPositions[blockHeight] = Tuple.Create(recordPosition, recordSize);
                    appendDataSize += recordSize;
                }

                if (cancellation != null)
                    cancellation.Token.ThrowIfCancellationRequested();

                await _ioDataStructureFileLockStream.FlushAsync().ConfigureAwait(false);
                _ioDataStructureFileLockStream.Flush(true);

                // Commit all in-memory metadata together, after the complete batch is durable.
                foreach (KeyValuePair<long, Tuple<long, long>> entry in appendPositions)
                {
                    ClassCacheIoStructureObject structure = _ioStructureObjectsDictionary[entry.Key];
                    long previousSize = structure.IsWritten ? structure.IoDataSizeOnFile : 0;
                    _totalIoCacheFileSize = Math.Max(0, _totalIoCacheFileSize - previousSize) + entry.Value.Item2;
                    structure.IoDataPosition = entry.Value.Item1;
                    structure.IoDataSizeOnFile = entry.Value.Item2;
                    structure.IsWritten = true;
                    blockListObject.Remove(entry.Key);
                }

                _lastIoCacheFileLength = _ioDataStructureFileLockStream.Length;
                _lastIoCacheTotalFileSizeWritten += appendDataSize;
                appendCommitted = true;
                return true;
            }
            catch (OperationCanceledException)
            {
                return false;
            }
            catch (Exception)
            {
                // If the atomic rename already happened, the new disk image is committed
                // even if reopening the stream or cleanup subsequently failed.
                return replacementCommitted;
            }
            finally
            {
                if (fullPurge && !replacementCommitted && File.Exists(temporaryPath))
                {
                    try { File.Delete(temporaryPath); }
                    catch { /* A stale temp file is harmless; the original remains authoritative. */ }
                }

                if (!replacementCommitted && !appendCommitted && !fullPurge &&
                    _ioDataStructureFileLockStream != null)
                {
                    try
                    {
                        // Do not extend a file that was already shorter than the committed
                        // index; that condition requires recovery, not zero-padding.
                        if (_ioDataStructureFileLockStream.Length >= _lastIoCacheFileLength)
                        {
                            _ioDataStructureFileLockStream.SetLength(_lastIoCacheFileLength);
                            _ioDataStructureFileLockStream.Position = _lastIoCacheFileLength;
                            await _ioDataStructureFileLockStream.FlushAsync().ConfigureAwait(false);
                        }
                    }
                    catch
                    {
                        // The committed length remains unchanged; startup indexing must
                        // validate the file before trusting any trailing bytes.
                    }
                }

                _ioOnWriteData = false;

                if (_ioStreamsClosed || _ioDataStructureFileLockStream == null)
                {
                    try { OpenLockStream(); }
                    catch { /* Preserve the original failure; a later operation may retry. */ }
                }
            }
        }

        /// <summary>
        /// Write data by block.
        /// </summary>
        /// <param name="data"></param>
        /// <param name="cancellation"></param>
        /// <param name="ioFileStream"></param>
        /// <returns></returns>
        private async Task<Tuple<bool, long>> WriteByBlock(byte[] data, CancellationTokenSource cancellation, FileStream ioFileStream)
        {
            if (data == null || ioFileStream == null || !ioFileStream.CanWrite)
                return new Tuple<bool, long>(false, 0);

            long totalWritten = 0;
            int percentWrite = _blockchainDatabaseSetting.BlockchainCacheSetting.IoCacheDiskMinPercentWriteFromBlockDataSize;
            long configuredChunkSize = ((long)data.Length * percentWrite) / 100;
            if (configuredChunkSize <= 0)
                configuredChunkSize = _blockchainDatabaseSetting.BlockchainCacheSetting.IoCacheDiskMinWriteByBlockSize;

            // Keep chunk sizes valid for array/Stream APIs and avoid allocating a new array per chunk.
            int chunkSize = (int)Math.Max(1, Math.Min((long)data.Length, Math.Min(configuredChunkSize, int.MaxValue)));

            try
            {
                while (totalWritten < data.Length)
                {
                    if (cancellation != null)
                        cancellation.Token.ThrowIfCancellationRequested();

                    int count = (int)Math.Min(chunkSize, data.Length - totalWritten);
                    if (cancellation == null)
                        await ioFileStream.WriteAsync(data, (int)totalWritten, count).ConfigureAwait(false);
                    else
                        await ioFileStream.WriteAsync(data, (int)totalWritten, count, cancellation.Token).ConfigureAwait(false);

                    totalWritten += count;
                }

                return new Tuple<bool, long>(true, totalWritten);
            }
            catch (OperationCanceledException)
            {
                return new Tuple<bool, long>(false, totalWritten);
            }
            catch (IOException)
            {
                return new Tuple<bool, long>(false, totalWritten);
            }
            catch (ObjectDisposedException)
            {
                return new Tuple<bool, long>(false, totalWritten);
            }
            catch (Exception)
            {
                return new Tuple<bool, long>(false, totalWritten);
            }
        }

        #endregion

        #region Manage stream functions.

        /// <summary>
        /// Close the lock stream of the io file cache.
        /// </summary>
        public void CloseLockStream()
        {
            // Idempotent: cleanup may be requested after a failed open/close path.
            if (_ioStreamsClosed && _ioDataStructureFileLockStream == null)
                return;

            FileStream stream = _ioDataStructureFileLockStream;
            _ioDataStructureFileLockStream = null;
            _ioStreamsClosed = true;
            if (stream != null)
                stream.Dispose();
        }

        /// <summary>
        /// Open a lock stream to the io file cache.
        /// </summary>
        private void OpenLockStream()
        {
            if (!_ioStreamsClosed && _ioDataStructureFileLockStream != null)
                return;

            // Assign only after the constructor succeeds, so a failed open does not
            // leave the object claiming to own a usable stream.
            FileStream stream = new FileStream(
                _ioDataStructureFilePath,
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                FileShare.Read,
                _blockchainDatabaseSetting.BlockchainCacheSetting.IoCacheDiskReadStreamBufferSize,
                FileOptions.Asynchronous | FileOptions.RandomAccess);

            _ioDataStructureFileLockStream = stream;
            _ioStreamsClosed = false;
        }

        /// <summary>
        /// Reset the seek of the lock stream of the io file cache.
        /// </summary>
        private async Task ResetSeekLockStreamAsync(CancellationTokenSource cancellation)
        {
            if (_ioStreamsClosed)
                OpenLockStream();

            _ioDataStructureFileLockStream.Position = 0;

            if (_ioDataStructureFileLockStream.Position != 0)
            {
                while (_ioDataStructureFileLockStream.Position != 0)
                {
                    if (cancellation != null)
                    {
                        if (cancellation.IsCancellationRequested)
                            break;

                        try
                        {
                            await Task.Delay(_blockchainDatabaseSetting.BlockchainCacheSetting.IoCacheDiskParallelTaskWaitDelay, cancellation.Token);
                        }
                        catch
                        {
                            break;
                        }
                    }
                    else
                        await Task.Delay(_blockchainDatabaseSetting.BlockchainCacheSetting.IoCacheDiskParallelTaskWaitDelay);

                    if (_ioDataStructureFileLockStream.Position == 0)
                        break;

                    _ioDataStructureFileLockStream.Position = 0;
                }
            }
        }

        /// <summary>
        /// Wait a io write done.
        /// </summary>
        /// <returns></returns>
        private async Task WaitIoWriteDone(CancellationTokenSource cancellation)
        {
            // Never continue while a write is active merely because the caller was
            // cancelled. Doing so could let a reader seek on the same FileStream.
            while (_ioOnWriteData)
            {
                if (cancellation != null)
                    cancellation.Token.ThrowIfCancellationRequested();

                int delay = Math.Max(1, _blockchainDatabaseSetting.BlockchainCacheSetting.IoCacheDiskParallelTaskWaitDelay);
                if (cancellation == null)
                    await Task.Delay(delay).ConfigureAwait(false);
                else
                    await Task.Delay(delay, cancellation.Token).ConfigureAwait(false);
            }

            if (cancellation != null)
                cancellation.Token.ThrowIfCancellationRequested();
        }

        #endregion

    }
}
