using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Logging;
using Xunit;
using ZiggyCreatures.Caching.Fusion;
using ZiggyCreatures.Caching.Fusion.Chaos;
using ZiggyCreatures.Caching.Fusion.Locking;
using ZiggyCreatures.Caching.Fusion.Locking.Distributed.Memory;
using ZiggyCreatures.Caching.Fusion.Serialization.SystemTextJson;

namespace FusionCacheTests;

public class BackgroundFactoryFallbackReproductionTests
{
	[Theory]
	[InlineData(false, false)]
	[InlineData(false, true)]
	[InlineData(true, false)]
	[InlineData(true, true)]
	public async Task ForegroundFallbackDoesNotOverwriteCompletedBackgroundValue(bool useAsync, bool fallbackFromL2)
	{
		using var cache = new FusionCache(new FusionCacheOptions
		{
			DisableTagging = true,
			EnableSyncEventHandlersExecution = true,
			DefaultEntryOptions = new FusionCacheEntryOptions
			{
				Duration = TimeSpan.FromMinutes(1),
				IsFailSafeEnabled = true,
				FactorySoftTimeout = TimeSpan.FromMilliseconds(50),
				FactoryHardTimeout = TimeSpan.FromMilliseconds(50)
			}
		});
		if (fallbackFromL2)
		{
			var storage = new MemoryDistributedCache(Options.Create(new MemoryDistributedCacheOptions()));
			cache.SetupDistributedCache(storage, new FusionCacheSystemTextJsonSerializer());
			var seed = cache.DefaultEntryOptions.Duplicate();
			seed.Duration = TimeSpan.Zero;
			seed.SkipMemoryCacheWrite = true;
			seed.AllowBackgroundDistributedCacheOperations = false;
			await cache.SetAsync("foo", 0, seed, token: TestContext.Current.CancellationToken);
		}
		var finish = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
		var background = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
		var fallback = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
		var resume = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
		cache.Events.BackgroundFactorySuccess += (_, _) => background.TrySetResult(true);
		cache.Events.FailSafeActivate += (_, _) =>
		{
			finish.TrySetResult(1);
			fallback.TrySetResult(true);
			resume.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken).GetAwaiter().GetResult();
		};
		var foreground = Task.Run(async () => useAsync
			? await cache.GetOrSetAsync("foo", _ => finish.Task, failSafeDefaultValue: fallbackFromL2 ? default : (MaybeValue<int>)0, token: TestContext.Current.CancellationToken)
			: cache.GetOrSet("foo", _ => finish.Task.GetAwaiter().GetResult(), failSafeDefaultValue: fallbackFromL2 ? default : (MaybeValue<int>)0, token: TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);
		try
		{
			await fallback.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
			// Give an incorrectly early background publication a bounded opportunity to finish.
			await Task.WhenAny(background.Task, Task.Delay(250, TestContext.Current.CancellationToken));
		}
		finally
		{
			resume.TrySetResult(true);
			finish.TrySetResult(1);
		}
		Assert.Equal(0, await foreground.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
		await background.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
		Assert.Equal(1, await cache.GetOrDefaultAsync<int>("foo", token: TestContext.Current.CancellationToken));
	}

	[Theory]
	[InlineData(false, false)]
	[InlineData(false, true)]
	[InlineData(true, false)]
	[InlineData(true, true)]
	public async Task NoFallbackPreservesBackgroundCompletionOption(bool useAsync, bool allowBackground)
	{
		using var cache = new FusionCache(new FusionCacheOptions
		{
			DisableTagging = true,
			EnableSyncEventHandlersExecution = true,
			DefaultEntryOptions = new FusionCacheEntryOptions
			{
				Duration = TimeSpan.FromMinutes(1),
				FactoryHardTimeout = TimeSpan.FromMilliseconds(50),
				AllowTimedOutFactoryBackgroundCompletion = allowBackground
			}
		});
		var finish = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
		var background = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
		cache.Events.BackgroundFactorySuccess += (_, _) => background.TrySetResult(true);
		try
		{
			await Assert.ThrowsAsync<SyntheticTimeoutException>(async () =>
			{
				if (useAsync)
					await cache.GetOrSetAsync("foo", _ => finish.Task, token: TestContext.Current.CancellationToken);
				else
					cache.GetOrSet("foo", _ => finish.Task.GetAwaiter().GetResult(), token: TestContext.Current.CancellationToken);
			});
			if (allowBackground)
			{
				finish.TrySetResult(1);
				await background.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
				Assert.Equal(1, await cache.GetOrDefaultAsync<int>("foo", token: TestContext.Current.CancellationToken));
				await cache.RemoveAsync("foo", token: TestContext.Current.CancellationToken);
			}
			using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
			Assert.Equal(2, await cache.GetOrSetAsync("foo", _ => Task.FromResult(2), token: timeout.Token));
			if (!allowBackground)
			{
				finish.TrySetResult(1);
				await Task.WhenAny(background.Task, Task.Delay(250, TestContext.Current.CancellationToken));
				Assert.False(background.Task.IsCompleted);
				Assert.Equal(2, await cache.GetOrDefaultAsync<int>("foo", token: TestContext.Current.CancellationToken));
			}
		}
		finally
		{
			finish.TrySetResult(1);
		}
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public async Task FailedFallbackWriteStillAllowsBackgroundCompletion(bool useAsync)
	{
		using var memory = new MemoryCache(new MemoryCacheOptions());
		var chaos = new ChaosMemoryCache(memory);
		using var cache = new FusionCache(new FusionCacheOptions
		{
			DisableTagging = true,
			EnableSyncEventHandlersExecution = true,
			DefaultEntryOptions = new FusionCacheEntryOptions
			{
				Duration = TimeSpan.FromMinutes(1),
				IsFailSafeEnabled = true,
				FactoryHardTimeout = TimeSpan.FromMilliseconds(50)
			}
		}, memoryCache: chaos);
		var finish = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
		var background = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
		cache.Events.BackgroundFactorySuccess += (_, _) => background.TrySetResult(true);
		cache.Events.FailSafeActivate += (_, _) => chaos.SetAlwaysThrow();
		try
		{
			await Assert.ThrowsAsync<ChaosException>(async () =>
			{
				if (useAsync)
					await cache.GetOrSetAsync("foo", _ => finish.Task, failSafeDefaultValue: 0, token: TestContext.Current.CancellationToken);
				else
					cache.GetOrSet("foo", _ => finish.Task.GetAwaiter().GetResult(), failSafeDefaultValue: 0, token: TestContext.Current.CancellationToken);
			});
		}
		finally
		{
			chaos.SetNeverThrow();
			finish.TrySetResult(1);
		}
		await background.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
		Assert.Equal(1, await cache.GetOrDefaultAsync<int>("foo", token: TestContext.Current.CancellationToken));
		await cache.RemoveAsync("foo", token: TestContext.Current.CancellationToken);
		using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
		Assert.Equal(2, await cache.GetOrSetAsync("foo", _ => Task.FromResult(2), token: timeout.Token));
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public async Task FailedForegroundWriteReleasesDistributedLock(bool useAsync)
	{
		using var memory = new MemoryCache(new MemoryCacheOptions());
		var chaos = new ChaosMemoryCache(memory);
		var storage = new MemoryDistributedCache(Options.Create(new MemoryDistributedCacheOptions()));
		var locker = new MemoryDistributedLocker(new MemoryDistributedLockerOptions());
		try
		{
			using var owner = new FusionCache(new FusionCacheOptions { DisableTagging = true }, memoryCache: chaos);
			using var contender = new FusionCache(new FusionCacheOptions { DisableTagging = true });
			owner.SetupDistributedCache(storage, new FusionCacheSystemTextJsonSerializer());
			contender.SetupDistributedCache(storage, new FusionCacheSystemTextJsonSerializer());
			owner.SetupDistributedLocker(locker);
			contender.SetupDistributedLocker(locker);
			int Factory()
			{
				chaos.SetAlwaysThrow();
				return 1;
			}
			try
			{
				await Assert.ThrowsAsync<ChaosException>(async () =>
				{
					if (useAsync)
						await owner.GetOrSetAsync("foo", _ => Task.FromResult(Factory()), token: TestContext.Current.CancellationToken);
					else
						owner.GetOrSet("foo", _ => Factory(), token: TestContext.Current.CancellationToken);
				});
			}
			finally
			{
				chaos.SetNeverThrow();
			}
			using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
			Assert.Equal(2, await contender.GetOrSetAsync("foo", _ => Task.FromResult(2), token: timeout.Token));
		}
		finally
		{
			locker.Dispose();
		}
	}

	[Theory]
	[InlineData(false, false)]
	[InlineData(false, true)]
	[InlineData(true, false)]
	[InlineData(true, true)]
	public async Task FactoryFailureDuringFallbackRaisesBackgroundError(bool useAsync, bool explicitFailure)
	{
		using var cache = new FusionCache(new FusionCacheOptions
		{
			DisableTagging = true,
			EnableSyncEventHandlersExecution = true,
			DefaultEntryOptions = new FusionCacheEntryOptions
			{
				IsFailSafeEnabled = true,
				FactoryHardTimeout = TimeSpan.FromMilliseconds(50)
			}
		});
		var finish = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
		var background = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
		FusionCacheFactoryExecutionContext<int>? context = null;
		var errors = 0;
		cache.Events.BackgroundFactoryError += (_, _) =>
		{
			Interlocked.Increment(ref errors);
			background.TrySetResult(true);
		};
		cache.Events.FailSafeActivate += (_, _) =>
		{
			if (explicitFailure)
				finish.TrySetResult(context!.Fail("Expected background failure"));
			else
				finish.TrySetException(new InvalidOperationException("Expected background failure"));
		};
		try
		{
			var value = useAsync
				? await cache.GetOrSetAsync<int>("foo", (ctx, _) => { context = ctx; return finish.Task; }, failSafeDefaultValue: 0, token: TestContext.Current.CancellationToken)
				: cache.GetOrSet<int>("foo", (ctx, _) => { context = ctx; return finish.Task.GetAwaiter().GetResult(); }, failSafeDefaultValue: 0, token: TestContext.Current.CancellationToken);
			Assert.Equal(0, value);
			await background.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
			await cache.RemoveAsync("foo", token: TestContext.Current.CancellationToken);
			using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
			Assert.Equal(2, await cache.GetOrSetAsync("foo", _ => Task.FromResult(2), token: timeout.Token));
			Assert.Equal(1, Volatile.Read(ref errors));
		}
		finally
		{
			finish.TrySetResult(1);
		}
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public async Task BackgroundCompletionKeepsMemoryLockUntilFactorySettles(bool useAsync)
	{
		using var locker = new CountingMemoryLocker();
		using var cache = new FusionCache(new FusionCacheOptions
		{
			DisableTagging = true,
			EnableSyncEventHandlersExecution = true,
			DefaultEntryOptions = new FusionCacheEntryOptions
			{
				Duration = TimeSpan.FromMinutes(1),
				IsFailSafeEnabled = true,
				FailSafeThrottleDuration = TimeSpan.Zero,
				FactoryHardTimeout = TimeSpan.FromMilliseconds(50)
			}
		}, memoryLocker: locker);
		var finish = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
		var background = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
		cache.Events.BackgroundFactorySuccess += (_, _) => background.TrySetResult(true);
		Task<int>? contender = null;
		var contenderCalls = 0;
		try
		{
			var value = useAsync
				? await cache.GetOrSetAsync("foo", _ => finish.Task, failSafeDefaultValue: 0, token: TestContext.Current.CancellationToken)
				: cache.GetOrSet("foo", _ => finish.Task.GetAwaiter().GetResult(), failSafeDefaultValue: 0, token: TestContext.Current.CancellationToken);
			Assert.Equal(0, value);
			Assert.Equal(0, locker.ReleaseCount);
			var options = cache.DefaultEntryOptions.Duplicate();
			options.IsFailSafeEnabled = false;
			options.FactoryHardTimeout = Timeout.InfiniteTimeSpan;
			options.LockTimeout = TimeSpan.FromSeconds(5);
			contender = Task.Run(async () => useAsync
				? await cache.GetOrSetAsync("foo", _ => Task.FromResult(Interlocked.Increment(ref contenderCalls) + 1), options, token: TestContext.Current.CancellationToken)
				: cache.GetOrSet("foo", _ => Interlocked.Increment(ref contenderCalls) + 1, options, token: TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);
			await locker.ContenderWaiting.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
			Assert.Equal(0, locker.ReleaseCount);
			finish.TrySetResult(1);
			await background.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
			Assert.Equal(1, await contender.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
			Assert.Equal(0, Volatile.Read(ref contenderCalls));
			Assert.Equal(2, locker.ReleaseCount);
		}
		finally
		{
			finish.TrySetResult(1);
			if (contender is not null)
				await contender.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
		}
	}

	// Each test uses one key, so a single semaphore implements the public locker contract.
	private sealed class CountingMemoryLocker : IFusionCacheMemoryLocker
	{
		private readonly SemaphoreSlim _gate = new(1, 1);
		private int _acquires;
		private int _releases;
		public int ReleaseCount => Volatile.Read(ref _releases);
		public TaskCompletionSource<bool> ContenderWaiting { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

		public async ValueTask<object?> AcquireLockAsync(string cacheName, string cacheInstanceId, string operationId, string key, TimeSpan timeout, ILogger? logger, CancellationToken token)
		{
			if (Interlocked.Increment(ref _acquires) == 2)
				ContenderWaiting.TrySetResult(true);
			return await _gate.WaitAsync(timeout, token) ? _gate : null;
		}

		public object? AcquireLock(string cacheName, string cacheInstanceId, string operationId, string key, TimeSpan timeout, ILogger? logger, CancellationToken token)
		{
			if (Interlocked.Increment(ref _acquires) == 2)
				ContenderWaiting.TrySetResult(true);
			return _gate.Wait(timeout, token) ? _gate : null;
		}

		public void ReleaseLock(string cacheName, string cacheInstanceId, string operationId, string key, object? lockObj, ILogger? logger)
		{
			if (lockObj is null)
				return;
			Interlocked.Increment(ref _releases);
			_gate.Release();
		}

		public void Dispose() => _gate.Dispose();
	}
}
