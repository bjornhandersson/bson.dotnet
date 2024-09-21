using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace bson.Dispatcher
{
    public class AsyncDispatcher : IAsyncDispatcher
    {
        private readonly int _partitionCount;
        private readonly int _maxCapacity;
        private readonly Channel<Func<CancellationToken, Task>>[] _partitions;
        private readonly Task[] _workers;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly DispatcherOptions _options;

        public AsyncDispatcher(DispatcherOptions? options = null)
        {
            _options = options ??= new DispatcherOptions();
            _partitionCount = _options.Partitions;
            _maxCapacity = _options.MaxCapacity;
            _partitions = new Channel<Func<CancellationToken, Task>>[_partitionCount];
            _workers = new Task[_partitionCount];
            _cancellationTokenSource = new CancellationTokenSource();

            for (int i = 0; i < _partitionCount; i++)
            {
                int localIndex = i;
                _partitions[i] = Channel.CreateBounded<Func<CancellationToken, Task>>(_maxCapacity);
                _workers[i] = Task.Factory.StartNew(
                    () => ProcessPartition(localIndex, _cancellationTokenSource.Token),
                    TaskCreationOptions.LongRunning
                );
            }
        }

        public async Task EnqueueAsync(int partitionKey, Func<CancellationToken, Task> action)
        {
            await _partitions[GetPartitionKey(partitionKey)].Writer.WriteAsync(action);
        }

        public async Task StopAsync()
        {
            _cancellationTokenSource.Cancel();
            await Task.WhenAll(_workers);
        }

        private async Task ProcessPartition(int partition, CancellationToken cancellationToken)
        {
            var channel = _partitions[partition];
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var workFunc = await channel.Reader.ReadAsync(cancellationToken);

                    using var taskCancellation = CancellationTokenSource.CreateLinkedTokenSource(
                        cancellationToken
                    );

                    if (_options.TaskTimeout != TimeSpan.MinValue)
                    {
                        taskCancellation.CancelAfter(_options.TaskTimeout);
                    }

                    await workFunc(taskCancellation.Token);
                }
                catch (Exception ex)
                {
                    // Let third party handle / log the exception
                    _options.ExceptionHandler.Invoke(ex);
                }
            }
        }

        public void Dispose()
        {
            _cancellationTokenSource.Cancel();
            // Guard against "deadlock" when the same thread that created the dispatcher is Disposing it.
            Task.Run(() => StopAsync()).Wait();
        }

        private int GetPartitionKey(int key)
        {
            return key % _partitionCount;
        }
    }
}
