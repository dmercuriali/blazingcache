/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */
package blazingcache;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import blazingcache.client.CacheClient;
import blazingcache.network.ServerHostData;
import blazingcache.network.netty.NettyCacheServerLocator;
import blazingcache.server.CacheServer;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.io.File;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Test;

public class SimpleSSLTest {

    @Test
    public void basicTestSslSelfSigned() throws Exception {
        basicTestSsl(null, null, null);
    }

    @Test
    public void basicTestSslWithCert() throws Exception {
        SelfSignedCertificate ssc = new SelfSignedCertificate();
        basicTestSsl(ssc.privateKey(), ssc.certificate(), null);
    }

    @Test
    public void basicTestSslWithPwdProtectedCert() throws Exception {
        File cert = new File(this.getClass().getClassLoader().getResource("cert1.key").getFile());
        File chain = new File(this.getClass().getClassLoader().getResource("cert1_chain.pem").getFile());
        basicTestSsl(cert, chain, "blazingcache1");
    }

    @Test
    public void basicTestSslWithPKCS12() throws Exception {
        File cert = new File(this.getClass().getClassLoader().getResource("cert1.p12").getFile());
        basicTestSsl(cert, null, "blazingcache1");
    }

    private void basicTestSsl(File certificateFile, File certificateChain, String certificateFilePassword) throws Exception {
        byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);

        ServerHostData serverHostData = new ServerHostData("localhost", 1234, "test", true, null);
        try (CacheServer cacheServer = new CacheServer("ciao", serverHostData)) {
            cacheServer.setupSsl(certificateFile, certificateFilePassword, certificateChain, null);
            cacheServer.start();
            try (CacheClient client1 = new CacheClient("theClient1", "ciao", new NettyCacheServerLocator(serverHostData));
                 CacheClient client2 = new CacheClient("theClient2", "ciao", new NettyCacheServerLocator(serverHostData));) {
                client1.start();
                client2.start();
                assertTrue(client1.waitForConnection(10000));
                assertTrue(client2.waitForConnection(10000));

                client1.put("pippo", data, 0);
                client2.put("pippo", data, 0);

                Assert.assertArrayEquals(data, client1.get("pippo").getSerializedData());
                Assert.assertArrayEquals(data, client2.get("pippo").getSerializedData());

                client1.invalidate("pippo");
                assertNull(client1.get("pippo"));
                assertNull(client2.get("pippo"));

            }

        }

    }

}
