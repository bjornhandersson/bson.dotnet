using System;

namespace bson.Dispatcher
{
    public class DispatcherOptions
    {
        /// <summary>
        /// Number of partitions to distribute the tasks. This is the degree of parallelism.
        /// @default <see cref="Environment.ProcessorCount"/>
        /// </summary>
        /// <value></value>
        public int Partitions { get; set; } = Environment.ProcessorCount;

        /// <summary>
        /// Max capacity per partition before blocking the producer.
        /// @default <see cref="int.MaxValue"/> = unbounded.
        /// </summary>
        /// <value></value>
        public int MaxCapacity { get; set; } = int.MaxValue;

        /// <summary>
        /// Maximum time to wait for a task to complete before aborting it.
        /// @default <see cref="TimeSpan.MinValue"/> = no timeout.
        /// </summary>
        /// <value></value>
        public TimeSpan TaskTimeout { get; set; } = TimeSpan.MinValue;

        /// <summary>
        /// Allow third party to handle exceptions that occurs within the task.
        /// </summary>
        /// <value></value>
        public Action<Exception> ExceptionHandler { get; set; } = (_) => { };

        public PartitionKeyAlgorithm PartitionKeyAlgorithm { get; set; } =
            PartitionKeyAlgorithm.Murmur2;
    }
}
