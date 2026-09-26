using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Xunit;
using ZiggyCreatures.Caching.Fusion;
using ZiggyCreatures.Caching.Fusion.Chaos;
using ZiggyCreatures.Caching.Fusion.Locking;
using ZiggyCreatures.Caching.Fusion.Locking.Distributed;
using ZiggyCreatures.Caching.Fusion.Locking.Distributed.Memory;
using ZiggyCreatures.Caching.Fusion.Serialization;
using ZiggyCreatures.Caching.Fusion.Serialization.SystemTextJson;

namespace FusionCacheTests;

public class DistributedLockAdversarialTests
	: IDisposable
{
	private readonly ObservedLocker _locker = new();

	public void Dispose()
	{
		_locker.Dispose();
	}

	[Theory]
	[InlineData(false, false)]
	[InlineData(false, true)]
	[InlineData(true, false)]
	[InlineData(true, true)]
	public async Task BackgroundFactoryKeepsLockUntilCompletion(bool useAsync, bool fail)
	{
		using var cache = CreateCache();
		cache.DefaultEntryOptions.FactoryHardTimeout = TimeSpan.FromMilliseconds(100);
		cache.DefaultEntryOptions.AllowTimedOutFactoryBackgroundCompletion = true;
		var started = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
		var finish = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
		var foreground = Task.Run(async () => await Record.ExceptionAsync(async () =>
		{
			if (useAsync)
				await cache.GetOrSetAsync<int>("foo", _ => { started.TrySetResult(true); return finish.Task; }, token: TestContext.Current.CancellationToken);
			else
				cache.GetOrSet<int>("foo", _ => { started.TrySetResult(true); return finish.Task.GetAwaiter().GetResult(); }, token: TestContext.Current.CancellationToken);
		}), TestContext.Current.CancellationToken);
		using var contender = CreateCache();
		Task<int>? refill = null;
		try
		{
			await started.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
			Assert.NotNull(await foreground.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
			refill = contender.GetOrSetAsync("foo", _ => Task.FromResult(2), token: TestContext.Current.CancellationToken).AsTask();
			Assert.False(refill.IsCompleted);
		}
		finally
		{
			if (fail)
				finish.TrySetException(new InvalidOperationException("injected factory failure"));
			else
				finish.TrySetResult(1);
		}
		Assert.Equal(2, await refill!.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
		Assert.Equal(2, _locker.Acquired);
		Assert.Equal(2, _locker.Released);
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public async Task ConcurrentNodesRunOneFactoryPerKey(bool mixedSync)
	{
		var l2 = new MemoryDistributedCache(Options.Create(new MemoryDistributedCacheOptions()));
		var caches = Enumerable.Range(0, 8).Select(_ => CreateCache(l2)).ToArray();
		try
		{
			for (var round = 0; round < 40; round++)
			{
				var key = "key-" + round;
				var calls = 0;
				var start = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
				var tasks = caches.Select((cache, index) => Task.Run(async () =>
				{
					await start.Task;
					if (mixedSync && index % 2 == 0)
						return cache.GetOrSet(key, _ => Interlocked.Increment(ref calls), token: TestContext.Current.CancellationToken);
					return await cache.GetOrSetAsync(key, async _ => { await Task.Yield(); return Interlocked.Increment(ref calls); }, token: TestContext.Current.CancellationToken);
				}, TestContext.Current.CancellationToken)).ToArray();
				start.SetResult(true);
				var results = await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
				Assert.All(results, value => Assert.Equal(1, value));
				Assert.Equal(1, calls);
			}
			Assert.Equal(_locker.Acquired, _locker.Released);
		}
		finally
		{
			foreach (var cache in caches)
				cache.Dispose();
		}
	}

	[Theory]
	[InlineData(false, false)]
	[InlineData(false, true)]
	[InlineData(true, false)]
	[InlineData(true, true)]
	public async Task ReleaseFailureAfterSideEffectMustNotReleaseTheNextOwner(bool useAsync, bool rethrow)
	{
		using var owner = CreateCache();
		using var contender = CreateCache();
		using var third = CreateCache();
		owner.DefaultEntryOptions.ReThrowDistributedLockerExceptions = rethrow;
		var started = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
		var finish = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
		Task<int>? second = null;
		_locker.AfterRelease = number =>
		{
			if (number != 1)
				return;
			second = contender.GetOrSetAsync("foo", _ =>
			{
				started.TrySetResult(true);
				return finish.Task;
			}, token: TestContext.Current.CancellationToken).AsTask();
			started.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken).GetAwaiter().GetResult();
			throw new IOException("release failed after its side effect");
		};
		Task<int>? thirdCall = null;
		try
		{
			var error = await Record.ExceptionAsync(async () =>
			{
				if (useAsync)
					await owner.GetOrSetAsync<int>("foo", _ => throw new InvalidOperationException("factory failed"), token: TestContext.Current.CancellationToken);
				else
					owner.GetOrSet<int>("foo", _ => throw new InvalidOperationException("factory failed"), token: TestContext.Current.CancellationToken);
			});
			if (rethrow)
				Assert.IsType<IOException>(error);
			else
				Assert.IsType<InvalidOperationException>(error);
			thirdCall = third.GetOrSetAsync("foo", _ => Task.FromResult(3), token: TestContext.Current.CancellationToken).AsTask();
			await Task.WhenAny(thirdCall, Task.Delay(100, TestContext.Current.CancellationToken));
			Assert.False(thirdCall.IsCompleted, "A failed release was retried after another node acquired the lock.");
			Assert.Equal(1, _locker.Released);
		}
		finally
		{
			finish.TrySetResult(2);
			if (second is not null)
				await second.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
			if (thirdCall is not null)
				await thirdCall.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
		}
	}

	[Theory]
	[InlineData(false, false)]
	[InlineData(false, true)]
	[InlineData(true, false)]
	[InlineData(true, true)]
	public async Task ReleaseFailureBeforeSideEffectIsLeftToTheProvider(bool useAsync, bool rethrow)
	{
		using var owner = CreateCache();
		owner.DefaultEntryOptions.ReThrowDistributedLockerExceptions = rethrow;
		_locker.BeforeRelease = () => throw new IOException("release failed before its side effect");
		var error = await Record.ExceptionAsync(async () =>
		{
			if (useAsync)
				await owner.GetOrSetAsync<int>("foo", _ => throw new InvalidOperationException("factory failed"), token: TestContext.Current.CancellationToken);
			else
				owner.GetOrSet<int>("foo", _ => throw new InvalidOperationException("factory failed"), token: TestContext.Current.CancellationToken);
		});
		if (rethrow)
			Assert.IsType<IOException>(error);
		else
			Assert.IsType<InvalidOperationException>(error);
		Assert.Equal(1, _locker.ReleaseAttempts);
		// A provider that fails before releasing must recover the handle itself.
		using var contender = CreateCache();
		using var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
			await contender.GetOrSetAsync("foo", _ => Task.FromResult(2), token: timeout.Token));
		Assert.Equal(1, _locker.Acquired);
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public async Task BackgroundMemoryReleaseFailureIsNotRetried(bool useAsync)
	{
		using var memoryLocker = new ReleaseFaultMemoryLocker();
		using var cache = CreateCache(memoryLocker: memoryLocker);
		cache.DefaultEntryOptions.EagerRefreshThreshold = 0.001f;
		var seed = cache.DefaultEntryOptions.Duplicate();
		seed.SkipDistributedCacheWrite = true;
		await cache.SetAsync("foo", 0, seed, token: TestContext.Current.CancellationToken);
		var completed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
		cache.Events.BackgroundFactorySuccess += (_, _) => completed.TrySetResult(true);
		await Task.Delay(100, TestContext.Current.CancellationToken);
		if (useAsync)
			await cache.GetOrSetAsync("foo", _ => Task.FromResult(1), token: TestContext.Current.CancellationToken);
		else
			cache.GetOrSet("foo", _ => 1, token: TestContext.Current.CancellationToken);
		await completed.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
		await Task.WhenAny(memoryLocker.Retried.Task, Task.Delay(200, TestContext.Current.CancellationToken));
		Assert.Equal(1, memoryLocker.ReleaseAttempts);
		Assert.Equal(1, await cache.GetOrDefaultAsync<int>("foo", token: TestContext.Current.CancellationToken));
	}

	private FusionCache CreateCache(IDistributedCache? l2 = null, IFusionCacheSerializer? serializer = null, IFusionCacheMemoryLocker? memoryLocker = null)
	{
		var cache = new FusionCache(new FusionCacheOptions
		{
			DisableTagging = true,
			EnableAutoRecovery = false,
			EnableSyncEventHandlersExecution = true,
			ReThrowOriginalExceptions = true,
			DefaultEntryOptions = new FusionCacheEntryOptions
			{
				Duration = TimeSpan.FromMinutes(1),
				AllowBackgroundDistributedCacheOperations = false,
				AllowBackgroundBackplaneOperations = false
			}
		}, memoryLocker: memoryLocker);
		cache.SetupDistributedCache(l2 ?? new MemoryDistributedCache(Options.Create(new MemoryDistributedCacheOptions())), serializer ?? new FusionCacheSystemTextJsonSerializer());
		cache.SetupDistributedLocker(_locker);
		return cache;
	}

	private sealed class ObservedLocker : IFusionCacheDistributedLocker, IDisposable
	{
		private readonly MemoryDistributedLocker _inner = new(new MemoryDistributedLockerOptions());
		public bool HonorCleanupCancellation { get; set; }
		public int Acquired;
		public int Released;
		public int ReleaseAttempts;
		public Action? BeforeRelease { get; set; }
		public Action<int>? AfterRelease { get; set; }
		public TaskCompletionSource<bool> SecondRelease { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

		public object? AcquireLock(string cacheName, string cacheInstanceId, string operationId, string key, string lockName, TimeSpan timeout, ILogger? logger, CancellationToken token)
		{
			var result = _inner.AcquireLock(cacheName, cacheInstanceId, operationId, key, lockName, timeout, logger, token);
			if (result is not null)
				Interlocked.Increment(ref Acquired);
			return result;
		}

		public async ValueTask<object?> AcquireLockAsync(string cacheName, string cacheInstanceId, string operationId, string key, string lockName, TimeSpan timeout, ILogger? logger, CancellationToken token)
		{
			var result = await _inner.AcquireLockAsync(cacheName, cacheInstanceId, operationId, key, lockName, timeout, logger, token);
			if (result is not null)
				Interlocked.Increment(ref Acquired);
			return result;
		}

		public void ReleaseLock(string cacheName, string cacheInstanceId, string operationId, string key, string lockName, object? lockObj, ILogger? logger, CancellationToken token)
		{
			if (HonorCleanupCancellation)
				token.ThrowIfCancellationRequested();
			Interlocked.Increment(ref ReleaseAttempts);
			BeforeRelease?.Invoke();
			var number = Interlocked.Increment(ref Released);
			_inner.ReleaseLock(cacheName, cacheInstanceId, operationId, key, lockName, lockObj, logger, token);
			AfterRelease?.Invoke(number);
			if (number == 2)
				SecondRelease.TrySetResult(true);
		}

		public ValueTask ReleaseLockAsync(string cacheName, string cacheInstanceId, string operationId, string key, string lockName, object? lockObj, ILogger? logger, CancellationToken token)
		{
			ReleaseLock(cacheName, cacheInstanceId, operationId, key, lockName, lockObj, logger, token);
			return ValueTask.CompletedTask;
		}

		public void Dispose()
		{
			_inner.Dispose();
		}
	}

	private sealed class ReleaseFaultMemoryLocker : IFusionCacheMemoryLocker
	{
		private readonly SemaphoreSlim _semaphore = new(1, 1);
		public int ReleaseAttempts;
		public TaskCompletionSource<bool> Retried { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

		public object? AcquireLock(string cacheName, string cacheInstanceId, string operationId, string key, TimeSpan timeout, ILogger? logger, CancellationToken token)
		{
			return _semaphore.Wait(timeout, token) ? _semaphore : null;
		}

		public async ValueTask<object?> AcquireLockAsync(string cacheName, string cacheInstanceId, string operationId, string key, TimeSpan timeout, ILogger? logger, CancellationToken token)
		{
			return await _semaphore.WaitAsync(timeout, token) ? _semaphore : null;
		}

		public void ReleaseLock(string cacheName, string cacheInstanceId, string operationId, string key, object? lockObj, ILogger? logger)
		{
			if (Interlocked.Increment(ref ReleaseAttempts) > 1)
				Retried.TrySetResult(true);
			_semaphore.Release();
			throw new InvalidOperationException("memory release failed after its side effect");
		}

		public void Dispose() => _semaphore.Dispose();
	}
}
