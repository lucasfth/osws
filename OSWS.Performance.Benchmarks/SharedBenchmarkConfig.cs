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
        private static int DefaultIterationCount => 5;

        /// <summary>
        /// Number of warmup iterations to use for all jobs.  Benchmarks can reference
        /// this value when deciding whether a particular run should be recorded.
        /// </summary>
        public static int DefaultWarmupCount => 1;

        /// <summary>
        /// Resolve iteration count from BENCH_ITERATIONS or BenchmarkDotNet CLI args.
        /// </summary>
        public static int GetConfiguredIterationCount()
        {
            if (
                int.TryParse(
                    Environment.GetEnvironmentVariable("BENCH_ITERATIONS"),
                    out var envIter
                )
                && envIter > 0
            )
            {
                return envIter;
            }

            var args = Environment.GetCommandLineArgs();
            for (var i = 0; i < args.Length; i++)
            {
                if (
                    string.Equals(args[i], "--iterationCount", StringComparison.OrdinalIgnoreCase)
                    && i + 1 < args.Length
                    && int.TryParse(args[i + 1], out var argIter)
                    && argIter > 0
                )
                {
                    return argIter;
                }

                const string prefix = "--iterationCount=";
                if (
                    args[i].StartsWith(prefix, StringComparison.OrdinalIgnoreCase)
                    && int.TryParse(args[i][prefix.Length..], out var inlineArgIter)
                    && inlineArgIter > 0
                )
                {
                    return inlineArgIter;
                }
            }

            return DefaultIterationCount;
        }

        public SharedBenchmarkConfig()
        {
            // Default: 5 iterations for statistical reliability
            // Override via BENCH_ITERATIONS environment variable or --iterationCount CLI flag
            var defaultIterations = GetConfiguredIterationCount();
            var warmupCount = DefaultWarmupCount;

            var defaultJob = Job
                .Default.WithWarmupCount(warmupCount)
                .WithIterationCount(defaultIterations);
            AddJob(defaultJob);
        }
    }
}
