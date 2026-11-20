using Xenophyte_Lib.Other.Object.Network;
using Xenophyte_Lib.Utility;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Xenophyte_Lib.TaskManager.Object
{
    public class ClassTaskObject
    {
        public long Id;
        public bool Started;
        public bool Disposed;
        private Func<Task> _action;
        public Task Task;
        public CancellationTokenSource Cancellation;
        public long TimestampEnd;
        public ClassCustomSocket Socket;

        /// <summary>
        /// Constructor.
        /// </summary>
        public ClassTaskObject(Func<Task> action, CancellationTokenSource cancellation, long timestampEnd, ClassCustomSocket socket)
        {
            Id = ClassUtility.GetRandomBetweenLong(0, long.MaxValue - 1);
            Cancellation = cancellation;
            TimestampEnd = timestampEnd;
            Socket = socket;
            _action = action;
        }

        /// <summary>
        /// Run the action into a task.
        /// </summary>
        public void Run()
        {
            try
            {
                // Always enter the delegate, even if cancellation was requested while this
                // task was queued. The linked token is still passed to the operation,
                // but scheduling with it could prevent the delegate's finally blocks
                // (including synchronization batch completion counters) from running.
                Task = Task.Factory.StartNew(_action, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).Unwrap();
            }
            catch
            {
                // Catch the exception, once the task is canelled.
            }
            Started = true;
        }
    }
}
