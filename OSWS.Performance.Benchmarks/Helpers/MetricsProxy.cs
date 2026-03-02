using System.Diagnostics;
using System.Reflection;

namespace OSWS.Performance.Benchmarks.Helpers
{
    /// <summary>
    /// Dispatch proxy that wraps an interface implementation and records
    /// call count and latency in a <see cref="MetricsCollector"/>.
    /// </summary>
    public class MetricsProxy<T> : DispatchProxy
        where T : class
    {
        private T? _decorated;
        private MetricsCollector? _metrics;

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod == null)
                throw new ArgumentNullException(nameof(targetMethod));

            if (_decorated == null)
                throw new InvalidOperationException("Proxy not initialized");

            var sw = Stopwatch.StartNew();
            try
            {
                var result = targetMethod.Invoke(_decorated, args);
                // if the result is a Task, we need to await it to capture latency
                if (result is System.Threading.Tasks.Task task)
                {
                    var taskType = task.GetType();
                    if (taskType.IsGenericType)
                    {
                        // Task<TResult>
                        var genericArg = taskType.GetGenericArguments()[0];
                        var handler = typeof(MetricsProxy<T>)
                            .GetMethod(
                                nameof(HandleGenericTask),
                                BindingFlags.NonPublic | BindingFlags.Instance
                            )!
                            .MakeGenericMethod(genericArg);
                        return handler.Invoke(this, new object[] { task, sw });
                    }

                    return AwaitAndRecordAsync(task, sw);
                }

                return result;
            }
            catch (TargetInvocationException tie)
            {
                // unwrap
                throw tie.InnerException ?? tie;
            }
        }

        private async System.Threading.Tasks.Task AwaitAndRecordAsync(
            System.Threading.Tasks.Task task,
            Stopwatch sw
        )
        {
            try
            {
                await task.ConfigureAwait(false);
            }
            finally
            {
                sw.Stop();
                Record(sw.Elapsed);
            }
        }

        private async System.Threading.Tasks.Task<TResult> HandleGenericTask<TResult>(
            System.Threading.Tasks.Task<TResult> task,
            Stopwatch sw
        )
        {
            try
            {
                return await task.ConfigureAwait(false);
            }
            finally
            {
                sw.Stop();
                Record(sw.Elapsed);
            }
        }

        private void Record(TimeSpan elapsed)
        {
            // decide which counter to increment based on interface type
            var type = typeof(T);
            if (type == typeof(OSWS.Models.Interfaces.IKeyVaultProvider))
            {
                _metrics?.RecordAzureKvCall(elapsed);
            }
            else if (type == typeof(Amazon.S3.IAmazonS3))
            {
                _metrics?.RecordS3Call(elapsed);
            }
            else
            {
                // generic operations could be logged if desired
            }
        }

        public static T Create(T decorated, MetricsCollector metrics)
        {
            var proxy = Create<T, MetricsProxy<T>>() as MetricsProxy<T>;
            proxy!._decorated = decorated;
            proxy!._metrics = metrics;
            return proxy as T ?? throw new InvalidOperationException("Failed to create proxy");
        }
    }
}
