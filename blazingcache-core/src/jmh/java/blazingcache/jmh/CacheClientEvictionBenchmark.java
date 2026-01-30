package blazingcache.jmh;

import blazingcache.client.CacheClient;
import blazingcache.client.ClientSideCache;
import blazingcache.client.ClientSideLRUCache;
import blazingcache.client.ClientSideSimpleCache;
import blazingcache.client.EntryHandle;
import blazingcache.utils.RawString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 30)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class CacheClientEvictionBenchmark {

    private ClientSideCache cache;
    private static final int ENTRIES = 100000;
    private static final int ttl = 10;
    private static final int maxSize = 16;

    @Param({"SIMPLE", "LRU"})
    public CacheClient.CacheImplementation impl;

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {

    }

    @Setup(Level.Invocation)
    public void setupInvocation(Blackhole bh) throws Exception {

        switch (impl) {
            case SIMPLE:
                cache = new ClientSideSimpleCache(null);
                break;
            case LRU:
                cache = new ClientSideLRUCache(0, null);
        }
        cache.setTtl(ttl);
        cache.setMaxSize(maxSize);

        long oldTime = System.currentTimeMillis() - ttl * 2;
        // fill the cache
        for (int i = 0; i < ENTRIES; i++) {
            String key = "benchmark-key-" + i;
            byte[] data = key.getBytes(StandardCharsets.UTF_8);
            ByteBuf buffer = UnpooledByteBufAllocator.DEFAULT.heapBuffer(data.length);
            buffer.writeBytes(data);
            EntryHandle entryHandle = new EntryHandle(new RawString(data), oldTime, buffer, 0, key);
            cache.store(entryHandle);
        }

        // do some gets to scramble the last get time
        Random rand = new Random();
        for (int i = 0; i < ENTRIES / 2; i++) {
            Thread.sleep(1);
            int k = rand.nextInt(ENTRIES) + 1;
            String key = "benchmark-key-" + k;
            byte[] data = key.getBytes(StandardCharsets.UTF_8);

            bh.consume(cache.getAndRetain(new RawString(data)));
        }
    }

    @TearDown(Level.Invocation)
    public void teardownInvocation() throws Exception {
        if (cache != null) {
            cache.clear();
        }
    }

    @Benchmark
    public void benchmarkEviction(Blackhole blackhole) throws Exception {
        Collection<RawString> rawStrings = cache.performEviction();
        blackhole.consume(rawStrings);
    }
}