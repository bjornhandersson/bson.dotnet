using System;
using System.Threading;
using System.Threading.Tasks;

namespace bson.Dispatcher
{
    public interface IAsyncDispatcher : IDisposable
    {
        ValueTask EnqueueAsync(int partition, Func<CancellationToken, ValueTask> action);

        ValueTask EnqueueAsync(byte[] partitionKey, Func<CancellationToken, ValueTask> action);

        Task StopAsync();
    }
}
