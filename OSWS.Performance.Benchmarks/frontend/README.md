# Performance Benchmarks Frontend

This is the frontend application for the OSWS performance benchmarks. It is built with React and Vite, and it loads the benchmark results from the `data` directory and displays them in a user-friendly way.

## Run

There are two ways to run the frontend:

### With Benchmarks Copying

To run against the newest benchmark results, you can use:

```bash
bun dev
```

This will also copy the latest benchmark results from the `BenchmarkDotNet.Artifacts`, and `warp-results` directories to the `data` directory, so the frontend can load them.

### Without Benchmarks Copying

If you just want to display the existing benchmark results in the `data` directory without copying new ones, you can use:

```bash
bun dev:existing
```

### Note

You can also manually copy the benchmark results to the `data` directory by running:

```bash
bun cp:benchmarks
```

This will copy the latest benchmark results from the `BenchmarkDotNet.Artifacts`, and `warp-results` directories to the `data` directory, so the frontend can load them.
