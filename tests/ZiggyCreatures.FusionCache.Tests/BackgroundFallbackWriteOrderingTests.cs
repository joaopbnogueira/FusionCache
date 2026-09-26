using Microsoft.Extensions.Caching.Memory;
using Xunit;
using ZiggyCreatures.Caching.Fusion;

namespace FusionCacheTests;

public class BackgroundFallbackWriteOrderingTests
{
	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public async Task BackgroundPublicationWaitsForForegroundMemoryWrite(bool useAsync)
	{
		using var memory = new GatedMemoryCache(TestContext.Current.CancellationToken);
		using var cache = new FusionCache(new FusionCacheOptions
		{
			DisableTagging = true,
			EnableSyncEventHandlersExecution = true,
			DefaultEntryOptions = new FusionCacheEntryOptions
			{
				IsFailSafeEnabled = true,
				FactoryHardTimeout = TimeSpan.FromMilliseconds(50)
			}
		}, memoryCache: memory);
		var finish = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
		var background = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
		cache.Events.BackgroundFactorySuccess += (_, _) => background.TrySetResult(true);
		cache.Events.FailSafeActivate += (_, _) =>
		{
			// Fail-safe activation and the foreground memory write execute on the same thread.
			memory.ForegroundThreadId = Environment.CurrentManagedThreadId;
			finish.TrySetResult(1);
		};
		var foreground = Task.Run(async () => useAsync
			? await cache.GetOrSetAsync("foo", _ => finish.Task, failSafeDefaultValue: 0, token: TestContext.Current.CancellationToken)
			: cache.GetOrSet("foo", _ => finish.Task.GetAwaiter().GetResult(), failSafeDefaultValue: 0, token: TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);
		try
		{
			await memory.WriteStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
			await Task.WhenAny(background.Task, Task.Delay(250, TestContext.Current.CancellationToken));
			Assert.False(background.Task.IsCompleted, "Background publication ran before the foreground memory write exited.");
		}
		finally
		{
			memory.AllowWrite.TrySetResult(true);
			finish.TrySetResult(1);
			await foreground.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
		}
		Assert.Equal(0, await foreground);
		await background.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
		Assert.Equal(1, await cache.GetOrDefaultAsync<int>("foo", token: TestContext.Current.CancellationToken));
	}

	private sealed class GatedMemoryCache(CancellationToken token) : IMemoryCache
	{
		private readonly MemoryCache _inner = new(new MemoryCacheOptions());
		private int _gateAvailable = 1;
		public int ForegroundThreadId;
		public TaskCompletionSource<bool> WriteStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
		public TaskCompletionSource<bool> AllowWrite { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

		public ICacheEntry CreateEntry(object key)
		{
			if (Environment.CurrentManagedThreadId == Volatile.Read(ref ForegroundThreadId) && Interlocked.Exchange(ref _gateAvailable, 0) == 1)
			{
				WriteStarted.TrySetResult(true);
				AllowWrite.Task.WaitAsync(TimeSpan.FromSeconds(5), token).GetAwaiter().GetResult();
			}
			return _inner.CreateEntry(key);
		}

		public bool TryGetValue(object key, out object? value) => _inner.TryGetValue(key, out value);
		public void Remove(object key) => _inner.Remove(key);
		public void Dispose() => _inner.Dispose();
	}
}
