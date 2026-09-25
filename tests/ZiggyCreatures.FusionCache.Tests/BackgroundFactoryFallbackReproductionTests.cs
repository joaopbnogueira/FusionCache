using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using Xunit;
using ZiggyCreatures.Caching.Fusion;
using ZiggyCreatures.Caching.Fusion.Serialization.SystemTextJson;

namespace FusionCacheTests;

public class BackgroundFactoryFallbackReproductionTests
{
	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public async Task ForegroundFallbackDoesNotOverwriteCompletedBackgroundValue(bool useAsync)
	{
		using var cache = new FusionCache(new FusionCacheOptions
		{
			DisableTagging = true,
			EnableSyncEventHandlersExecution = true,
			DefaultEntryOptions = new FusionCacheEntryOptions
			{
				Duration = TimeSpan.FromMinutes(1),
				IsFailSafeEnabled = true,
				FactorySoftTimeout = TimeSpan.FromMilliseconds(50)
			}
		});
		var storage = new MemoryDistributedCache(Options.Create(new MemoryDistributedCacheOptions()));
		cache.SetupDistributedCache(storage, new FusionCacheSystemTextJsonSerializer());
		var seed = cache.DefaultEntryOptions.Duplicate();
		seed.Duration = TimeSpan.FromMilliseconds(50);
		seed.SkipMemoryCacheWrite = true;
		seed.AllowBackgroundDistributedCacheOperations = false;
		await cache.SetAsync("foo", 0, seed, token: TestContext.Current.CancellationToken);
		await Task.Delay(100, TestContext.Current.CancellationToken);
		var finish = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
		var background = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
		cache.Events.BackgroundFactorySuccess += (_, _) => background.TrySetResult(true);
		cache.Events.FailSafeActivate += (_, _) =>
		{
			finish.TrySetResult(1);
			// Force background publication before the foreground stores its fallback.
			background.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken).GetAwaiter().GetResult();
		};
		try
		{
			if (useAsync)
				await cache.GetOrSetAsync("foo", _ => finish.Task, token: TestContext.Current.CancellationToken);
			else
				cache.GetOrSet("foo", _ => finish.Task.GetAwaiter().GetResult(), token: TestContext.Current.CancellationToken);
			Assert.True(background.Task.IsCompletedSuccessfully);
			Assert.Equal(1, await cache.GetOrDefaultAsync<int>("foo", token: TestContext.Current.CancellationToken));
		}
		finally
		{
			finish.TrySetResult(1);
		}
	}
}
