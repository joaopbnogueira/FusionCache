using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Xunit;
using ZiggyCreatures.Caching.Fusion;
using ZiggyCreatures.Caching.Fusion.Backplane.Memory;
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
	[InlineData(false)]
	[InlineData(true)]
	public async Task ForegroundMemoryReleaseFailureIsNotRetried(bool useAsync)
	{
		using var memoryLocker = new ReleaseFaultMemoryLocker();
		using var cache = CreateCache(memoryLocker: memoryLocker);
		await Assert.ThrowsAsync<InvalidOperationException>(async () =>
		{
			if (useAsync)
				await cache.GetOrSetAsync<int>("foo", _ => throw new InvalidOperationException("Factory failed"), token: TestContext.Current.CancellationToken);
			else
				cache.GetOrSet<int>("foo", _ => throw new InvalidOperationException("Factory failed"), token: TestContext.Current.CancellationToken);
		});
		Assert.Equal(1, memoryLocker.ReleaseAttempts);
	}

	[Theory]
	[InlineData(false, false)]
	[InlineData(false, true)]
	[InlineData(true, false)]
	[InlineData(true, true)]
	public async Task FactoryCancellationMustAllowCleanupWithACancellationAwareLocker(bool useAsync, bool honorCleanupCancellation)
	{
		_locker.HonorCleanupCancellation = honorCleanupCancellation;
		using var cache = CreateCache();
		using var cancellation = new CancellationTokenSource();
		int Factory()
		{
			cancellation.Cancel();
			throw new OperationCanceledException(cancellation.Token);
		}

		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
		{
			if (useAsync)
				await cache.GetOrSetAsync<int>("foo", _ => Task.FromResult(Factory()), token: cancellation.Token);
			else
				cache.GetOrSet<int>("foo", _ => Factory(), token: cancellation.Token);
		});

		await AssertRefillAsync();
	}

	[Theory]
	[InlineData(false, false)]
	[InlineData(false, true)]
	[InlineData(true, false)]
	[InlineData(true, true)]
	public async Task CancellationKeepsLockUntilProviderWriteSettles(bool useAsync, bool background)
	{
		var l2 = new GatedCache();
		using var cache = CreateCache(l2);
		cache.DefaultEntryOptions.AllowBackgroundDistributedCacheOperations = background;
		using var cancellation = new CancellationTokenSource();
		var first = Task.Run(async () => await Record.ExceptionAsync(async () =>
		{
			if (useAsync)
				await cache.GetOrSetAsync("foo", _ => Task.FromResult(1), token: cancellation.Token);
			else
				cache.GetOrSet("foo", _ => 1, token: cancellation.Token);
		}), TestContext.Current.CancellationToken);
		using var contender = CreateCache(l2);
		using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
		Task<int>? second = null;
		var factoryCalls = 0;
		var enteredBeforeWriteSettled = false;
		try
		{
			await l2.Started.Task.WaitAsync(timeout.Token);
			cancellation.Cancel();
			second = contender.GetOrSetAsync("foo", _ =>
			{
				Interlocked.Increment(ref factoryCalls);
				return Task.FromResult(2);
			}, token: timeout.Token).AsTask();
			await Task.WhenAny(second, Task.Delay(200, TestContext.Current.CancellationToken));
			enteredBeforeWriteSettled = Volatile.Read(ref factoryCalls) != 0;
		}
		finally
		{
			l2.AllowCompletion.TrySetResult(true);
			await l2.Completed.Task.WaitAsync(timeout.Token);
			var error = await first.WaitAsync(timeout.Token);
			if (background)
				Assert.Null(error);
			else
				Assert.IsAssignableFrom<OperationCanceledException>(error);
		}

		var secondValue = await second!.WaitAsync(timeout.Token);
		using var observer = CreateCache(l2);
		var storedValue = await observer.GetOrDefaultAsync<int>("foo", token: TestContext.Current.CancellationToken);
		Assert.False(enteredBeforeWriteSettled, $"A contender entered during the old write; it returned {secondValue}, but L2 ended at {storedValue}.");
		Assert.Equal(secondValue, storedValue);
		Assert.Equal(_locker.Acquired, _locker.Released);
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
	[InlineData(false)]
	[InlineData(true)]
	public async Task CanceledWaiterDoesNotReleaseTheOwnersLock(bool useAsync)
	{
		using var owner = CreateCache();
		using var waiter = CreateCache();
		using var contender = CreateCache();
		var acquired = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
		var complete = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
		var holding = owner.GetOrSetAsync("foo", _ => { acquired.SetResult(true); return complete.Task; }, token: TestContext.Current.CancellationToken).AsTask();
		await acquired.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
		using var cancel = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
		try
		{
			await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
			{
				if (useAsync)
					await waiter.GetOrSetAsync("foo", _ => Task.FromResult(2), token: cancel.Token);
				else
					waiter.GetOrSet("foo", _ => 2, token: cancel.Token);
			});
			Assert.Equal(1, _locker.Acquired);
			Assert.Equal(0, _locker.Released);
		}
		finally
		{
			complete.TrySetResult(1);
			await holding;
		}
		await AssertRefillAsync();
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
	public async Task PublicationCancellationRaceDoesNotLeak(bool useAsync, bool background)
	{
		using var cache = CreateCache();
		using var contender = CreateCache();
		cache.DefaultEntryOptions.AllowBackgroundDistributedCacheOperations = background;
		CancellationTokenSource? current = null;
		Task cancellationTask = Task.CompletedTask;
		cache.Events.Memory.Set += (_, _) =>
		{
			var cancellation = current!;
			cancellationTask = Task.Run(async () => { await Task.Yield(); cancellation.Cancel(); });
		};
		for (var iteration = 0; iteration < 100; iteration++)
		{
			using var cancellation = new CancellationTokenSource();
			current = cancellation;
			var key = "race-" + iteration;
			var error = await Record.ExceptionAsync(async () =>
			{
				if (useAsync)
					await cache.GetOrSetAsync(key, _ => Task.FromResult(1), token: cancellation.Token);
				else
					cache.GetOrSet(key, _ => 1, token: cancellation.Token);
			});
			await cancellationTask;
			Assert.True(error is null or OperationCanceledException);
			using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
			Assert.Equal(2, await contender.GetOrSetAsync(key, _ => Task.FromResult(2), token: timeout.Token));
		}
		Assert.Equal(200, _locker.Acquired);
		Assert.Equal(200, _locker.Released);
	}

	[Theory]
	[InlineData(false, false)]
	[InlineData(false, true)]
	[InlineData(true, false)]
	[InlineData(true, true)]
	public async Task CancellationDuringSerializationKeepsPublicationOwnership(bool useAsync, bool background)
	{
		using var cancellation = new CancellationTokenSource();
		var l2 = new MemoryDistributedCache(Options.Create(new MemoryDistributedCacheOptions()));
		using var owner = CreateCache(l2, new CallbackSerializer(cancellation.Cancel));
		owner.DefaultEntryOptions.AllowBackgroundDistributedCacheOperations = background;
		var error = await Record.ExceptionAsync(async () =>
		{
			if (useAsync)
				await owner.GetOrSetAsync("foo", _ => Task.FromResult(1), token: cancellation.Token);
			else
				owner.GetOrSet("foo", _ => 1, token: cancellation.Token);
		});
		if (background)
			Assert.Null(error);
		else
			Assert.IsAssignableFrom<OperationCanceledException>(error);
		using var contender = CreateCache(l2);
		using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
		Assert.Equal(1, await contender.GetOrSetAsync("foo", _ => Task.FromResult(2), token: timeout.Token));
		Assert.True(cancellation.IsCancellationRequested);
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
	public async Task OnlyLockedPublicationUsesAnIndependentProviderToken(bool locked)
	{
		var l2 = new GatedCache();
		l2.AllowCompletion.TrySetResult(true);
		using var cache = CreateCache(l2);
		if (!locked)
			cache.RemoveDistributedLocker();
		using var cancellation = new CancellationTokenSource();
		Assert.Equal(1, await cache.GetOrSetAsync("foo", _ => Task.FromResult(1), token: cancellation.Token));
		Assert.Equal(!locked, l2.WriteTokenCanBeCanceled);
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

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public async Task UnlockedCancellationStillEndsTheWaitBeforeTheProvider(bool useAsync)
	{
		var l2 = new GatedCache();
		using var cache = CreateCache(l2);
		cache.RemoveDistributedLocker();
		using var cancellation = new CancellationTokenSource();
		var call = Task.Run(async () => await Record.ExceptionAsync(async () =>
		{
			if (useAsync)
				await cache.GetOrSetAsync("foo", _ => Task.FromResult(1), token: cancellation.Token);
			else
				cache.GetOrSet("foo", _ => 1, token: cancellation.Token);
		}), TestContext.Current.CancellationToken);
		try
		{
			await l2.Started.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
			cancellation.Cancel();
			Assert.IsAssignableFrom<OperationCanceledException>(await call.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
			Assert.False(l2.Completed.Task.IsCompleted);
			Assert.Equal(0, _locker.Acquired);
		}
		finally
		{
			l2.AllowCompletion.TrySetResult(true);
			await call.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
			await l2.Completed.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
		}
	}

	[Theory]
	[InlineData(false, false)]
	[InlineData(false, true)]
	[InlineData(true, false)]
	[InlineData(true, true)]
	public async Task CancelledLockedPublicationStillNotifiesOtherNodes(bool useAsync, bool background)
	{
		var l2 = new GatedCache();
		using var owner = CreateCache(l2);
		using var observer = CreateCache(l2);
		var seed = observer.DefaultEntryOptions.Duplicate();
		seed.SkipDistributedCacheWrite = true;
		await observer.SetAsync("foo", 0, seed, token: TestContext.Current.CancellationToken);
		var connectionId = Guid.NewGuid().ToString("N");
		owner.SetupBackplane(new MemoryBackplane(new MemoryBackplaneOptions { ConnectionId = connectionId }));
		observer.SetupBackplane(new MemoryBackplane(new MemoryBackplaneOptions { ConnectionId = connectionId }));
		owner.DefaultEntryOptions.AllowBackgroundDistributedCacheOperations = background;
		var published = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
		owner.Events.Backplane.MessagePublished += (_, _) => published.TrySetResult(true);
		using var cancellation = new CancellationTokenSource();
		var write = Task.Run(async () => await Record.ExceptionAsync(async () =>
		{
			if (useAsync)
				await owner.GetOrSetAsync("foo", _ => Task.FromResult(1), token: cancellation.Token);
			else
				owner.GetOrSet("foo", _ => 1, token: cancellation.Token);
		}), TestContext.Current.CancellationToken);
		try
		{
			await l2.Started.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
			Assert.Equal(0, await observer.GetOrDefaultAsync<int>("foo", token: TestContext.Current.CancellationToken));
			cancellation.Cancel();
			l2.AllowCompletion.TrySetResult(true);
			await l2.Completed.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
			await published.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
			var error = await write.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
			if (background)
				Assert.Null(error);
			else
				Assert.IsAssignableFrom<OperationCanceledException>(error);
			Assert.Equal(1, await observer.GetOrDefaultAsync<int>("foo", token: TestContext.Current.CancellationToken));
			Assert.Equal(1, _locker.ReleaseAttempts);
		}
		finally
		{
			l2.AllowCompletion.TrySetResult(true);
			await write.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
		}
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

	private async Task AssertRefillAsync()
	{
		using var contender = CreateCache();
		using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
		Assert.Equal(2, await contender.GetOrSetAsync("foo", _ => Task.FromResult(2), token: timeout.Token));
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

	private sealed class GatedCache : IDistributedCache
	{
		private readonly MemoryDistributedCache _inner = new(Options.Create(new MemoryDistributedCacheOptions()));
		private int _writes;
		public Action? BeforeRead { get; set; }
		public bool? WriteTokenCanBeCanceled { get; private set; }
		public TaskCompletionSource<bool> Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
		public TaskCompletionSource<bool> AllowCompletion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
		public TaskCompletionSource<bool> Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

		public byte[]? Get(string key)
		{
			BeforeRead?.Invoke();
			return _inner.Get(key);
		}

		public Task<byte[]?> GetAsync(string key, CancellationToken token = default)
		{
			BeforeRead?.Invoke();
			return _inner.GetAsync(key, token);
		}

		public void Set(string key, byte[] value, DistributedCacheEntryOptions options)
		{
			SetAsync(key, value, options).GetAwaiter().GetResult();
		}

		public async Task SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token = default)
		{
			WriteTokenCanBeCanceled = token.CanBeCanceled;
			var first = Interlocked.Increment(ref _writes) == 1;
			if (first)
			{
				Started.TrySetResult(true);
				// A remote command already sent can complete after local cancellation.
				await AllowCompletion.Task;
			}
			_inner.Set(key, value, options);
			if (first)
				Completed.TrySetResult(true);
		}

		public void Remove(string key)
		{
			_inner.Remove(key);
		}

		public Task RemoveAsync(string key, CancellationToken token = default)
		{
			return _inner.RemoveAsync(key, token);
		}

		public void Refresh(string key)
		{
			_inner.Refresh(key);
		}

		public Task RefreshAsync(string key, CancellationToken token = default)
		{
			return _inner.RefreshAsync(key, token);
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

	private sealed class CallbackSerializer(Action beforeSerialize) : IFusionCacheSerializer
	{
		private readonly FusionCacheSystemTextJsonSerializer _inner = new();

		public byte[] Serialize<T>(T? obj)
		{
			beforeSerialize();
			return _inner.Serialize(obj);
		}

		public ValueTask<byte[]> SerializeAsync<T>(T? obj, CancellationToken token = default)
		{
			beforeSerialize();
			return _inner.SerializeAsync(obj, token);
		}

		public T? Deserialize<T>(byte[] data) => _inner.Deserialize<T>(data);
		public ValueTask<T?> DeserializeAsync<T>(byte[] data, CancellationToken token = default) => _inner.DeserializeAsync<T>(data, token);
	}
}
