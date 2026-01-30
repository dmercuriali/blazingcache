package blazingcache.jmh;

import blazingcache.client.CacheClient;
import blazingcache.network.ServerHostData;
import blazingcache.network.netty.NettyCacheServerLocator;
import blazingcache.server.CacheServer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
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
public class CacheClientLoadBenchmark {

    private CacheServer cacheServer;
    private CacheClient cacheClient;
    private ServerHostData serverHostData;
    private static final String SHARED_SECRET = "ciao";
    private static final int PORT = 1234;

    @Param({"SIMPLE", "LRU"})
    public CacheClient.CacheImplementation impl;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        Logger.getLogger("").setLevel(java.util.logging.Level.OFF);
        // Setup server
        serverHostData = new ServerHostData("localhost", PORT, "test", false, null);
        cacheServer = new CacheServer(SHARED_SECRET, serverHostData);
        cacheServer.start();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (cacheClient != null) {
            cacheClient.close();
        }
        if (cacheServer != null) {
            cacheServer.close();
        }
    }

    @Setup(Level.Invocation)
    public void setupInvocation() throws Exception {
        // Setup client
        cacheClient = CacheClient.newBuilder()
                .clientId("theClient1")
                .sharedSecret(SHARED_SECRET)
                .serverLocator(new NettyCacheServerLocator(serverHostData))
                .implementation(impl)
                .build();
        cacheClient.start();

        // Wait for connection
        if (!cacheClient.waitForConnection(10000)) {
            throw new RuntimeException("Client failed to connect to server");
        }
    }

    @TearDown(Level.Invocation)
    public void teardownInvocation() throws Exception {
        if (cacheClient != null) {
            cacheClient.close();
            cacheClient = null;
        }
    }

    @Benchmark
    public void benchmarkLoad(Blackhole blackhole) throws Exception {
        String key = "benchmark-key-" + System.nanoTime();
        byte[] data = key.getBytes(StandardCharsets.UTF_8);
        boolean result = cacheClient.load(key, data, 0);
        blackhole.consume(result);
    }
}