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
            ArgumentNullException.ThrowIfNull(targetMethod);

            if (_decorated == null)
                throw new InvalidOperationException("Proxy not initialized");

            var sw = Stopwatch.StartNew();
            try
            {
                var result = targetMethod.Invoke(_decorated, args);
                // if the result is a Task, we need to await it to capture latency
                if (result is not Task task)
                    return result;
                var taskType = task.GetType();
                if (!taskType.IsGenericType)
                    return AwaitAndRecordAsync(task, sw);
                // Task<TResult>
                var genericArg = taskType.GetGenericArguments()[0];
                var handler = typeof(MetricsProxy<T>)
                    .GetMethod(
                        nameof(HandleGenericTask),
                        BindingFlags.NonPublic | BindingFlags.Instance
                    )!
                    .MakeGenericMethod(genericArg);
                return handler.Invoke(this, [task, sw]);
            }
            catch (TargetInvocationException tie)
            {
                // unwrap
                throw tie.InnerException ?? tie;
            }
        }

        private async Task AwaitAndRecordAsync(Task task, Stopwatch sw)
        {
            Exception? capturedException = null;
            try
            {
                await task.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                capturedException = ex;
            }
            finally
            {
                sw.Stop();
                Record(sw.Elapsed);
            }

            if (capturedException != null)
            {
                var inner = capturedException switch
                {
                    AggregateException ae => ae.InnerException ?? ae,
                    TargetInvocationException tie => tie.InnerException ?? tie,
                    _ => capturedException,
                };

                if (inner is TargetInvocationException targetInv)
                {
                    throw targetInv.InnerException ?? targetInv;
                }
                throw inner;
            }
        }

        private async Task<TResult> HandleGenericTask<TResult>(Task<TResult> task, Stopwatch sw)
        {
            Exception? capturedException = null;
            try
            {
                return await task.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                capturedException = ex;
                return default!;
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
            if (type == typeof(Models.Interfaces.IKeyVaultProvider))
            {
                _metrics?.RecordKvCall(elapsed);
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
