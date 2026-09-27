using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Xunit;
using ZiggyCreatures.Caching.Fusion;
using ZiggyCreatures.Caching.Fusion.Chaos;
using ZiggyCreatures.Caching.Fusion.Locking.Distributed;
using ZiggyCreatures.Caching.Fusion.Locking.Distributed.Memory;
using ZiggyCreatures.Caching.Fusion.Serialization.SystemTextJson;

namespace FusionCacheTests;

public class DistributedLockCleanupTests
	: IDisposable
{
	private readonly MemoryDistributedLocker _locker = new(new MemoryDistributedLockerOptions());

	public void Dispose()
	{
		_locker.Dispose();
	}

	[Theory]
	[InlineData(false, false)]
	[InlineData(false, true)]
	[InlineData(true, false)]
	[InlineData(true, true)]
	public async Task CanceledPublicationAllowsAnotherNodeToFill(bool useAsync, bool background)
	{
		using var cache = CreateCache(_locker);
		using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
		cache.DefaultEntryOptions.AllowBackgroundDistributedCacheOperations = background;
		cache.Events.Memory.Set += (_, _) => cancellation.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
		{
			if (useAsync)
				await cache.GetOrSetAsync("foo", _ => Task.FromResult(1), token: cancellation.Token);
			else
				cache.GetOrSet("foo", _ => 1, token: cancellation.Token);
		});

		await AssertAnotherNodeCanFillAsync(_locker, "foo");
	}

	[Theory]
	[InlineData(false, false)]
	[InlineData(false, true)]
	[InlineData(true, false)]
	[InlineData(true, true)]
	public async Task PublicationKeepsLockUntilL2WriteCompletes(bool useAsync, bool background)
	{
		var distributedCache = new GatedDistributedCache();
		using var cache = CreateCache(_locker, distributedCache);
		cache.DefaultEntryOptions.AllowBackgroundDistributedCacheOperations = background;
		var publication = Task.Run(async () =>
		{
			if (useAsync)
				await cache.GetOrSetAsync("foo", _ => Task.FromResult(1), token: TestContext.Current.CancellationToken);
			else
				cache.GetOrSet("foo", _ => 1, token: TestContext.Current.CancellationToken);
		}, TestContext.Current.CancellationToken);

		using var contender = CreateCache(_locker);
		using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
		timeout.CancelAfter(TimeSpan.FromSeconds(5));
		Task<int>? refill = null;
		try
		{
			await distributedCache.WriteStarted.Task.WaitAsync(timeout.Token);
			// The in-memory contender reaches the shared lock without asynchronous I/O.
			refill = contender.GetOrSetAsync("foo", _ => Task.FromResult(2), token: timeout.Token).AsTask();
			Assert.False(refill.IsCompleted);
		}
		finally
		{
			distributedCache.AllowWrite.TrySetResult(true);
			await publication.WaitAsync(timeout.Token);
		}

		Assert.NotNull(refill);
		Assert.Equal(2, await refill.WaitAsync(timeout.Token));
	}

	private static FusionCache CreateCache(IFusionCacheDistributedLocker locker, IDistributedCache? distributedCache = null)
	{
		var cache = new FusionCache(new FusionCacheOptions
		{
			DisableTagging = true,
			EnableAutoRecovery = false,
			EnableSyncEventHandlersExecution = true,
			ReThrowOriginalExceptions = true,
			DistributedCacheCircuitBreakerDuration = TimeSpan.FromMinutes(1),
			DefaultEntryOptions = new FusionCacheEntryOptions
			{
				Duration = TimeSpan.FromMinutes(1),
				AllowBackgroundDistributedCacheOperations = false,
				AllowBackgroundBackplaneOperations = false
			}
		});
		cache.SetupDistributedCache(distributedCache ?? new MemoryDistributedCache(Options.Create(new MemoryDistributedCacheOptions())), new FusionCacheSystemTextJsonSerializer());
		cache.SetupDistributedLocker(locker);
		return cache;
	}

	private static async Task AssertAnotherNodeCanFillAsync(IFusionCacheDistributedLocker locker, string key)
	{
		// A separate empty L2 forces the contender to acquire the shared lock and run its factory.
		using var contender = CreateCache(locker);
		using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
		timeout.CancelAfter(TimeSpan.FromSeconds(2));
		Assert.Equal(2, await contender.GetOrSetAsync(key, _ => Task.FromResult(2), token: timeout.Token));
	}

	private sealed class GatedDistributedCache
		: IDistributedCache
	{
		private readonly MemoryDistributedCache _inner = new(Options.Create(new MemoryDistributedCacheOptions()));

		public TaskCompletionSource<bool> WriteStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
		public TaskCompletionSource<bool> AllowWrite { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

		public byte[]? Get(string key)
		{
			return _inner.Get(key);
		}

		public Task<byte[]?> GetAsync(string key, CancellationToken token = default)
		{
			return _inner.GetAsync(key, token);
		}

		public void Refresh(string key)
		{
			_inner.Refresh(key);
		}

		public Task RefreshAsync(string key, CancellationToken token = default)
		{
			return _inner.RefreshAsync(key, token);
		}

		public void Remove(string key)
		{
			_inner.Remove(key);
		}

		public Task RemoveAsync(string key, CancellationToken token = default)
		{
			return _inner.RemoveAsync(key, token);
		}

		public void Set(string key, byte[] value, DistributedCacheEntryOptions options)
		{
			WriteStarted.TrySetResult(true);
			AllowWrite.Task.GetAwaiter().GetResult();
			_inner.Set(key, value, options);
		}

		public async Task SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token = default)
		{
			WriteStarted.TrySetResult(true);
			await AllowWrite.Task.WaitAsync(token).ConfigureAwait(false);
			await _inner.SetAsync(key, value, options, token).ConfigureAwait(false);
		}
	}
}
