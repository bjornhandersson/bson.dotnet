using System.Diagnostics;
using bson.Dispatcher;

namespace bson.Dispatcher.Test;

public class DispatcherTest
{
    [Test]
    public async Task Dispatcher_Should_execute_task_on_given_shard()
    {
        // Arrange
        int partitions = 2;
        int numberOfTasks = 1_000;
        var completed = new TaskCompletionSource<bool>();
        using IAsyncDispatcher dispatcher = new AsyncDispatcher(
            new DispatcherOptions { Partitions = partitions }
        );

        // Act
        var result = new int[partitions];
        int counter = 0;
        for (int i = 0; i < numberOfTasks; i++)
        {
            int partitionKey = i % partitions;
            int localIndex = i;
            await dispatcher.EnqueueAsync(
                partitionKey: i,
                (_) =>
                {
                    result[partitionKey]++;

                    if (Interlocked.Increment(ref counter) == numberOfTasks)
                    {
                        completed.SetResult(true);
                    }

                    return Task.CompletedTask;
                }
            );
        }

        await Task.WhenAny(completed.Task, Task.Delay(TimeSpan.FromSeconds(1)));

        // Assert
        for (int i = 0; i < partitions; i++)
        {
            Assert.That(result[i], Is.EqualTo(numberOfTasks / partitions));
        }
    }

    [Test]
    public async Task Dispatcher_Should_abort_long_running_tasks()
    {
        // Arrange
        using IAsyncDispatcher dispatcher = new AsyncDispatcher(
            new DispatcherOptions { TaskTimeout = TimeSpan.FromMilliseconds(500), Partitions = 2 }
        );

        // Act
        var stopwatch = Stopwatch.StartNew();
        await dispatcher.EnqueueAsync(
            partitionKey: 0,
            async (cancellationToken) =>
            {
                await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
            }
        );
        stopwatch.Stop();

        // Assert
        Assert.That(stopwatch.Elapsed, Is.LessThan(TimeSpan.FromSeconds(2)));

        // Can queue more work after task being cancelled
        TaskCompletionSource<bool> didExecute = new();
        await dispatcher.EnqueueAsync(
            partitionKey: 0,
            (cancellationToken) =>
            {
                didExecute.SetResult(true);
                return Task.CompletedTask;
            }
        );

        var completedTask = await Task.WhenAny(
            didExecute.Task,
            Task.Delay(TimeSpan.FromSeconds(10))
        );
        Assert.That(completedTask, Is.SameAs(didExecute.Task));
    }
}
