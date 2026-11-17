using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Threading;
using Xenophyte_Lib.Blockchain.Setting;
using Xenophyte_Lib.Instance.Node;
using Xenophyte_Lib.Log;
using Xenophyte_Lib.Utility;
using Xenophyte_Peer.CommandLine;

namespace Xenophyte_Peer
{
    class Program
    {

        private static ClassNodeInstance _nodeInstance;
        private static ClassConsoleCommandLine _consoleCommandLine;

        /// <summary>
        /// Entry point.
        /// </summary>
        static void Main()
        {
            AppDomain.CurrentDomain.UnhandledException += UnhandledException;
            Thread.CurrentThread.Name = Path.GetFileName(Environment.GetCommandLineArgs()[0]);
            _nodeInstance = new ClassNodeInstance();


            if (ClassLog.InitializeWriteLog())
            {
                ClassLog.EnableWriteLogTask();
                ClassLog.WriteLine(BlockchainSetting.CoinName + " Peer Tool " + Assembly.GetExecutingAssembly().GetName().Version, ClassEnumLogLevelType.LOG_LEVEL_GENERAL, ClassEnumLogWriteLevel.LOG_WRITE_LEVEL_MANDATORY_PRIORITY);

                bool peerLoadStatus = _nodeInstance.PeerInitializationSetting();

                if (!peerLoadStatus)
                {
                    ClassLog.WriteLine("Press a key to exit.", ClassEnumLogLevelType.LOG_LEVEL_GENERAL, ClassEnumLogWriteLevel.LOG_WRITE_LEVEL_LOWEST_PRIORITY);
                    Console.ReadLine();
                    Environment.Exit(0);
                }
                else
                {
                    if (_nodeInstance.NodeStart(false))
                    {
                        _consoleCommandLine = new ClassConsoleCommandLine(_nodeInstance);
                        _consoleCommandLine.EnableConsoleCommandLine();
                    }
                }

            }
            else
            {
                Console.WriteLine("Can't enable log write system. Press a key to exit.");
                Console.ReadLine();
            }
        }



        #region Other main functions.

        /// <summary>
        /// Catch unexpected exception not handled.
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            var filePath = ClassUtility.ConvertPath(AppDomain.CurrentDomain.BaseDirectory + "\\error_peer.txt");
            var exception = (Exception)e.ExceptionObject;
            using (var writer = new StreamWriter(filePath, true))
            {
                writer.WriteLine("Message :" + exception.Message + "<br/>" + Environment.NewLine +
                                 "StackTrace :" +
                                 exception.StackTrace +
                                 "" + Environment.NewLine + "Date :" + DateTime.Now);
                writer.WriteLine(Environment.NewLine +
                                 "-----------------------------------------------------------------------------" +
                                 Environment.NewLine);
            }

            Trace.TraceError(exception.StackTrace);
            _nodeInstance?.NodeStop(true);
            Environment.Exit(1);
        }

        #endregion
    }
}
