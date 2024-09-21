using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using bson.Dispatcher.Hash;

namespace bson.Dispatcher.Test
{
    public class AsyncDispatcherTest
    {
        [TestCase(PartitionKeyAlgorithm.MurmurHash2, 0.06)]
        [TestCase(PartitionKeyAlgorithm.BasicHash, 0.000001)]
        public void Hashes_Should_distribute_evenly(
            PartitionKeyAlgorithm keyAlgorithm,
            decimal tolerancePct
        )
        {
            IHashGenerator hashGenerator = keyAlgorithm switch
            {
                PartitionKeyAlgorithm.BasicHash => new BasicHash(),
                PartitionKeyAlgorithm.MurmurHash2 => new MurmurHash2(),
                _ => throw new ArgumentOutOfRangeException(
                    nameof(keyAlgorithm),
                    keyAlgorithm,
                    null
                ),
            };

            var result = new int[2];
            for (int i = 0; i < 10_000; i++)
            {
                uint hash = hashGenerator.Hash(BitConverter.GetBytes(i));
                result[hash % 2]++;
            }

            decimal diff = (decimal)Math.Abs(result[0] - result[1]);
            decimal pctOff = diff / 1000;

            // Tolerance in full percentage
            Assert.Less(pctOff, tolerancePct);
        }

        [Test]
        public async Task Dispatcher_Should_apply_backpressure_when_capacity_is_reached()
        {
            // Arrange
            int partitions = 2;
            int taskCompleted = 0;
            var completed = new TaskCompletionSource<bool>();
            using IAsyncDispatcher dispatcher = new AsyncDispatcher(
                new DispatcherOptions { Partitions = partitions, MaxCapacity = 1 }
            );

            // Act
            var result = new int[partitions];
            for (int i = 0; i < 4; i++)
            {
                int partition = i % partitions;
                // Fill up the dispatcher
                await dispatcher.EnqueueAsync(
                    partition,
                    async (_) =>
                    {
                        taskCompleted++;
                        await completed.Task;
                        Console.WriteLine("Task completed");
                    }
                );
            }

            // Ensure we timeout when enqueuing more tasks
            var enqueueTask = dispatcher.EnqueueAsync(
                partition: 0,
                async (_) =>
                {
                    taskCompleted++;
                    await completed.Task;
                }
            );

            var timeoutTask = Task.Delay(TimeSpan.FromSeconds(1));
            var completedTask = await Task.WhenAny(enqueueTask.AsTask(), timeoutTask);

            // Assert
            Assert.AreEqual(completedTask, timeoutTask, "Expect timeout");
            Assert.IsFalse(enqueueTask.IsCompleted);

            // Free up the dispatcher
            completed.SetResult(true);
            await completedTask;

            Assert.AreEqual(5, taskCompleted);
        }

        [Test]
        [Category("DispatcherPerformance")]
        [Ignore("For performance testing only")]
        public async Task Dispatcher_Should_handle_load_over_time()
        {
            int partitions = Environment.ProcessorCount;
            int taskCompleted = 0;
            int iterations = 1_000_000;
            var completed = new TaskCompletionSource<bool>();
            using IAsyncDispatcher dispatcher = new AsyncDispatcher(
                new DispatcherOptions { Partitions = partitions, MaxCapacity = 2000 }
            );

            var stopwatch = new Stopwatch();
            stopwatch.Start();
            for (int i = 0; i < iterations; i++)
            {
                int partition = i % partitions;
                await dispatcher.EnqueueAsync(
                    partition,
                    (_) =>
                    {
                        Thread.SpinWait(50_000); // Around 1 ms
                        if (Interlocked.Increment(ref taskCompleted) == iterations)
                        {
                            completed.SetResult(true);
                        }

                        return default;
                    }
                );
            }

            var completedTask = await Task.WhenAny(completed.Task, Task.Delay(30_000));
            if (completedTask != completed.Task)
            {
                Console.WriteLine($"Timed out. Task completed: {taskCompleted}");
            }

            stopwatch.Stop();
            Console.WriteLine(
                $"Elapsed time: {stopwatch.Elapsed.TotalMilliseconds} ms Completed tasks: {taskCompleted} CPU: {partitions}"
            );
        }

        [Test]
        public async Task Dispatcher_Should_abort_long_running_tasks()
        {
            // Arrange
            using IAsyncDispatcher dispatcher = new AsyncDispatcher(
                new DispatcherOptions
                {
                    TaskTimeout = TimeSpan.FromMilliseconds(500),
                    Partitions = 2,
                }
            );

            // Act
            var stopwatch = Stopwatch.StartNew();
            await dispatcher.EnqueueAsync(
                partition: 0,
                async (cancellationToken) =>
                {
                    await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
                }
            );
            stopwatch.Stop();

            // Assert
            Assert.That(stopwatch.Elapsed, Is.LessThan(TimeSpan.FromSeconds(2)));

            // Can queue more work after task being cancelled
            var didExecute = new TaskCompletionSource<bool>();
            await dispatcher.EnqueueAsync(
                partition: 0,
                (cancellationToken) =>
                {
                    didExecute.SetResult(true);
                    return default;
                }
            );

            var completedTask = await Task.WhenAny(
                didExecute.Task,
                Task.Delay(TimeSpan.FromSeconds(10))
            );
            Assert.That(completedTask, Is.SameAs(didExecute.Task));
        }
    }
}
