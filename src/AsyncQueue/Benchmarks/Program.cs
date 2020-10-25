using BenchmarkDotNet.Attributes;
using System;
using System.Threading.Tasks;
using Dejan.Jelovic.AsyncQueue;
using BenchmarkDotNet.Running;

[MemoryDiagnoser]
[WarmupCount(20)]
public class ReusableTaskCompletionSourceBenchmarks {
    [Benchmark]
    public async Task BenchmarkReusableTaskCompletionSource() {
        const int iterations = 1_000_0000;

        var left = new ReusableTaskCompletionSource<int>();
        var right = new ReusableTaskCompletionSource<int>();

        async Task TestLeft() {
            for (int x = 0; x < iterations; ++x) {
                left.SetResult(x);
                (await right.GetResultAsync().ConfigureAwait(false)).Expect(x);
            }
        }

        async Task TestRight() {
            for (int x = 0; x < iterations; ++x) {
                (await left.GetResultAsync().ConfigureAwait(false)).Expect(x);
                right.SetResult(x);
            }
        }

        var t1 = TestLeft();
        var t2 = TestRight();

        await t1;
        await t2;
    }
}

class Program {
    static void Main() => BenchmarkRunner.Run<ReusableTaskCompletionSourceBenchmarks>();
}
