namespace bson.Dispatcher;

public interface IAsyncDispatcher : IDisposable
{
    Task EnqueueAsync(int partitionKey, Func<CancellationToken, Task> action);
    Task StopAsync();
}
