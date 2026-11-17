using Xenophyte_Lib.Instance.Node.Setting.Object;
using Xenophyte_Lib.Log;
using Xenophyte_Lib.Other.Object.List;
using Xenophyte_Lib.Other.Object.Network;
using Xenophyte_Lib.TaskManager.Object;
using Xenophyte_Lib.Utility;
using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Xenophyte_Lib.TaskManager
{
    public static class TaskManager
    {
        private static volatile bool _enabled;
        private static readonly CancellationTokenSource _globalCts = new CancellationTokenSource();

        private static readonly List<ClassTaskObject> _tasks = new List<ClassTaskObject>(1024);
        private static readonly DisposableList<long> _completedTaskIds = new DisposableList<long>();

        private const int MaxCompletedBeforeCleanup = 5000;

        public static long CurrentTimestampMillisecond { get; private set; }
        public static long CurrentTimestampSecond { get; private set; }

        public static int CountTask
        {
            get
            {
                lock (_tasks)
                    return _tasks.Count;
            }
        }

        public static int CountTaskCompleted
        {
            get
            {
                int count = 0;
                lock (_tasks)
                {
                    for (int i = 0; i < _tasks.Count; i++)
                        if (_tasks[i]?.Disposed == true)
                            count++;
                }
                return count;
            }
        }

        #region ENABLE / STOP

        public static void EnableTaskManager(ClassPeerNetworkSettingObject settings)
        {
            if (_enabled)
                return;

            _enabled = true;

            StartTimestampUpdater();
            StartTaskCleaner();
            StartCompletedTaskCleanup();
        }

        public static void StopTaskManager()
        {
            _enabled = false;

            if (!_globalCts.IsCancellationRequested)
                _globalCts.Cancel();

            ForceCleanup();
        }

        #endregion

        #region BACKGROUND WORKERS

        private static void StartTimestampUpdater()
        {
            Task.Run(async () =>
            {
                while (_enabled)
                {
                    CurrentTimestampMillisecond = ClassUtility.GetCurrentTimestampInMillisecond();
                    CurrentTimestampSecond = ClassUtility.GetCurrentTimestampInSecond();
                    await Task.Delay(1).ConfigureAwait(false);
                }
            }, _globalCts.Token);
        }

        private static void StartTaskCleaner()
        {
            Task.Run(async () =>
            {
                while (_enabled)
                {
                    ManageTasks(false);
                    await Task.Delay(1000).ConfigureAwait(false);
                }
            }, _globalCts.Token);
        }

        private static void StartCompletedTaskCleanup()
        {
            Task.Run(async () =>
            {
                while (_enabled)
                {
                    CleanupCompletedTasks();
                    await Task.Delay(60000).ConfigureAwait(false);
                }
            }, _globalCts.Token);
        }

        #endregion

        #region CORE LOGIC

        private static void ManageTasks(bool force)
        {
            lock (_tasks)
            {
                for (int i = 0; i < _tasks.Count; i++)
                {
                    ClassTaskObject task = _tasks[i];
                    if (task == null || task.Disposed || !task.Started)
                        continue;

                    bool dispose = force ||
                                   task.Task == null ||
                                   task.Task.IsCompleted ||
                                   task.Task.IsCanceled ||
                                   task.Task.IsFaulted ||
                                   (task.TimestampEnd > 0 && task.TimestampEnd < CurrentTimestampMillisecond);

                    if (!dispose)
                        continue;

                    task.Disposed = true;

                    // Stop the work before disposing its task/token source.
                    try
                    {
                        if (task.Cancellation != null &&
                            !task.Cancellation.IsCancellationRequested)
                            task.Cancellation.Cancel();
                    }
                    catch { }

                    task.Socket?.Kill(SocketShutdown.Both);
                    try { task.Task?.Dispose(); } catch { }
                    try { task.Cancellation?.Dispose(); } catch { }

                    _completedTaskIds.Add(task.Id);
                }
            }
        }

        private static void CleanupCompletedTasks()
        {
            if (_completedTaskIds.Count < MaxCompletedBeforeCleanup)
                return;

            lock (_tasks)
            {
                for (int i = _tasks.Count - 1; i >= 0; i--)
                {
                    if (_tasks[i] == null || _tasks[i].Disposed)
                        _tasks.RemoveAt(i);
                }
            }

            _completedTaskIds.Clear();
        }

        private static void ForceCleanup()
        {
            ManageTasks(true);
            CleanupCompletedTasks();
        }

        #endregion

        #region INSERT TASK

        public static Task InsertTask(
            Action action,
            long timestampEnd,
            CancellationTokenSource cancellation,
            ClassCustomSocket socket = null,
            bool useFactory = false)
        {
            if (action == null)
                throw new ArgumentNullException(nameof(action));

            return InsertTask(
                () =>
                {
                    action();
                    return Task.CompletedTask;
                },
                timestampEnd,
                cancellation,
                socket,
                useFactory);
        }

        /// <summary>
        /// Schedules asynchronous work and tracks its real completion.
        /// Async delegates must be stored as Task-returning delegates;
        /// treating them as Action creates async-void tasks that appear
        /// completed before their network operations finish.
        /// </summary>
        public static async Task InsertTask(
            Func<Task> action,
            long timestampEnd,
            CancellationTokenSource cancellation,
            ClassCustomSocket socket = null,
            bool useFactory = false)
        {
            if (action == null)
                throw new ArgumentNullException(nameof(action));

            if (!_enabled)
                return;

            CancellationTokenSource localCts = null;

            try
            {
                localCts = cancellation != null
                    ? CancellationTokenSource.CreateLinkedTokenSource(
                        cancellation.Token,
                        _globalCts.Token)
                    : CancellationTokenSource.CreateLinkedTokenSource(
                        _globalCts.Token);

                if (timestampEnd > 0)
                {
                    long remaining = Math.Max(
                        0,
                        timestampEnd - CurrentTimestampMillisecond);

                    localCts.CancelAfter(
                        remaining > int.MaxValue
                            ? int.MaxValue
                            : (int)remaining);
                }
            }
            catch
            {
                localCts?.Dispose();
                return;
            }

            if (useFactory)
            {
                try
                {
                    await Task.Factory.StartNew(
                        action,
                        localCts.Token,
                        TaskCreationOptions.LongRunning,
                        TaskScheduler.Default).Unwrap().ConfigureAwait(false);
                }
                catch (OperationCanceledException) { }
                catch { }
                finally
                {
                    localCts.Dispose();
                }
                return;
            }

            lock (_tasks)
            {
                var taskObj = new ClassTaskObject(
                    action,
                    localCts,
                    timestampEnd,
                    socket);

                _tasks.Add(taskObj);

                if (!taskObj.Started)
                    taskObj.Run();
            }
        }

        #endregion
    }
}
