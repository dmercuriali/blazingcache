package blazingcache.client;

import blazingcache.client.impl.InternalClientListener;
import blazingcache.network.Channel;
import blazingcache.network.Message;
import blazingcache.network.ServerHostData;
import blazingcache.network.netty.NettyCacheServerLocator;
import blazingcache.server.CacheServer;
import java.nio.charset.StandardCharsets;
import static org.junit.Assert.*;
import org.junit.Test;

public class DisconnectOnTimeoutTest {

    @Test
    public void disconnectOnTimeout() throws Exception {
        byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);
        ServerHostData serverHostData = new ServerHostData("localhost", 1234, "test", false, null);
        try (CacheServer cacheServer = new CacheServer("ciao", serverHostData)) {
            cacheServer.setSlowClientTimeout(1000);
            cacheServer.setDisconnectClientsOnTimeout(true);
            cacheServer.start();
            try (CacheClient client1 = new CacheClient("theClient1", "ciao", new NettyCacheServerLocator(serverHostData));
                 CacheClient client2 = new CacheClient("theClient2", "ciao", new NettyCacheServerLocator(serverHostData))) {
                client1.start();
                client2.start();
                assertTrue(client1.waitForConnection(10000));
                assertTrue(client2.waitForConnection(10000));

                client1.setInternalClientListener(new InternalClientListener() {
                    @Override
                    public boolean messageReceived(Message message, Channel channel) {
                        if (message.type == Message.TYPE_FETCH_ENTRY) {
                            // ignore request to simulate timeout
                            return false;
                        }
                        return true;
                    }
                });

                client1.put("lost-fetch", data, 0);
                assertNull(client2.fetch("lost-fetch"));

                assertTrue(client1.waitForDisconnection(5000));

                for (int i = 0; i < 50; i++) {
                    if (cacheServer.getNumberOfConnectedClients() == 1) {
                        break;
                    }
                    Thread.sleep(100);
                }
                assertEquals(1, cacheServer.getNumberOfConnectedClients());
            }
        }
    }
}
