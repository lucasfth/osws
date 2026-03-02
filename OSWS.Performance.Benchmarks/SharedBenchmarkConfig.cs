using System;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;

namespace OSWS.Performance.Benchmarks
{
    /// <summary>
    /// Common configuration for all benchmarks in this solution.
    /// Iteration count can be controlled by environment variable
    /// BENCH_ITERATIONS or by passing the `--iterationCount` argument to
    /// the BenchmarkDotNet command line (via `dotnet run -- ...`).
    /// </summary>
    public class SharedBenchmarkConfig : ManualConfig
    {
        public SharedBenchmarkConfig()
        {
            // if user supplies CLI flag (--iterationCount X) BenchmarkDotNet will
            // override whatever job we add here, so this check is only a default
            // for when nothing is specified.
            var defaultJob = Job.Default;

            if (
                int.TryParse(Environment.GetEnvironmentVariable("BENCH_ITERATIONS"), out var iter)
                && iter > 0
            )
            {
                defaultJob = defaultJob.WithIterationCount(iter);
            }

            AddJob(defaultJob);
        }
    }
}
