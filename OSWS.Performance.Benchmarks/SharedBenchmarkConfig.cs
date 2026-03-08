using System;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;

namespace OSWS.Performance.Benchmarks
{
    /// <summary>
    /// Common configuration for all benchmarks in this solution.
    /// Provides statistical reliability with multiple iterations and percentile tracking.
    /// Iteration count can be controlled by environment variable BENCH_ITERATIONS or
    /// by passing the `--iterationCount` argument to the BenchmarkDotNet command line.
    /// </summary>
    [MemoryDiagnoser]
    public class SharedBenchmarkConfig : ManualConfig
    {
        public SharedBenchmarkConfig()
        {
            // Default: 5 iterations for statistical reliability
            // Override via BENCH_ITERATIONS environment variable or --iterationCount CLI flag
            var defaultIterations = 5;
            var warmupCount = 1;

            if (
                int.TryParse(Environment.GetEnvironmentVariable("BENCH_ITERATIONS"), out var iter)
                && iter > 0
            )
            {
                defaultIterations = iter;
            }

            var defaultJob = Job
                .Default.WithWarmupCount(warmupCount)
                .WithIterationCount(defaultIterations);
            AddJob(defaultJob);
        }
    }
}
