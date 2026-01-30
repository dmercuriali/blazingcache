package blazingcache.client;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import blazingcache.utils.RawString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;

public class ClientSideLRUCacheTest {

    @Test
    public void basicFunctionality() throws Exception {
        ClientSideLRUCache cache = new ClientSideLRUCache(10, null);

        assertThat(cache.size(), is(0));

        cache.store(createEntryHandle("key1", "value1".getBytes(StandardCharsets.UTF_8)));
        cache.store(createEntryHandle("key2", "value2".getBytes(StandardCharsets.UTF_8)));
        cache.store(createEntryHandle("key2", "value2".getBytes(StandardCharsets.UTF_8)));
        cache.store(createEntryHandle("key3", "value2".getBytes(StandardCharsets.UTF_8)));
        RawString key = new RawString("key1".getBytes(StandardCharsets.UTF_8));

        assertThat(cache.size(), is(3));

        EntryHandle entry = cache.getAndRetain(key);
        assertThat(new String(entry.getSerializedData(), StandardCharsets.UTF_8), is("value1"));
        assertThat(cache.size(), is(3));

        cache.remove(key);
        assertThat(cache.size(), is(2));

        entry = cache.getAndRetain(key);
        assertNull(entry);

        cache.clear();
        assertThat(cache.size(), is(0));
    }

    @Test
    public void whenStoreNullThenFail() {
        ClientSideLRUCache cache = new ClientSideLRUCache(10, null);

        try {
            cache.store(null);
            Assert.fail("should not insert null entry");
        } catch (IllegalArgumentException ex) {
            // ok
        }
    }

    @Test
    public void verifyLRUList() {
        ClientSideLRUCache cache = new ClientSideLRUCache(10, null);

        assertNull(cache.getLeastRecentlyUsedKey());
        assertNull(cache.getMostRecentlyUsedKey());

        cache.store(createEntryHandle("key1", "value1".getBytes(StandardCharsets.UTF_8)));
        {
            RawString head = cache.getMostRecentlyUsedKey();
            RawString tail = cache.getLeastRecentlyUsedKey();
            assertSame(head, tail);
            assertThat(cache.getLruKeysInOrder(), is(Arrays.asList(raw("key1"))));
        }

        cache.store(createEntryHandle("key2", "value2".getBytes(StandardCharsets.UTF_8)));
        {
            RawString head = cache.getMostRecentlyUsedKey();
            RawString tail = cache.getLeastRecentlyUsedKey();
            assertThat(head, is(new RawString("key2".getBytes(StandardCharsets.UTF_8))));
            assertThat(tail, is(new RawString("key1".getBytes(StandardCharsets.UTF_8))));
            assertThat(cache.getLruKeysInOrder(), is(Arrays.asList(raw("key2"), raw("key1"))));
        }

        cache.store(createEntryHandle("key3", "value3".getBytes(StandardCharsets.UTF_8)));
        {
            RawString head = cache.getMostRecentlyUsedKey();
            RawString tail = cache.getLeastRecentlyUsedKey();
            assertThat(head, is(new RawString("key3".getBytes(StandardCharsets.UTF_8))));
            assertThat(tail, is(new RawString("key1".getBytes(StandardCharsets.UTF_8))));
            assertThat(cache.getLruKeysInOrder(), is(Arrays.asList(raw("key3"), raw("key2"), raw("key1"))));
        }

        cache.getAndRetain(new RawString("key1".getBytes(StandardCharsets.UTF_8)));
        {
            RawString head = cache.getMostRecentlyUsedKey();
            RawString tail = cache.getLeastRecentlyUsedKey();
            assertThat(head, is(new RawString("key1".getBytes(StandardCharsets.UTF_8))));
            assertThat(tail, is(new RawString("key2".getBytes(StandardCharsets.UTF_8))));
            assertThat(cache.getLruKeysInOrder(), is(Arrays.asList(raw("key1"), raw("key3"), raw("key2"))));
        }

        cache.remove(new RawString("key3".getBytes(StandardCharsets.UTF_8)));
        {
            RawString head = cache.getMostRecentlyUsedKey();
            RawString tail = cache.getLeastRecentlyUsedKey();
            assertThat(head, is(new RawString("key1".getBytes(StandardCharsets.UTF_8))));
            assertThat(tail, is(new RawString("key2".getBytes(StandardCharsets.UTF_8))));
            assertThat(cache.getLruKeysInOrder(), is(Arrays.asList(raw("key1"), raw("key2"))));
        }

        cache.remove(new RawString("key2".getBytes(StandardCharsets.UTF_8)));
        {
            RawString head = cache.getMostRecentlyUsedKey();
            RawString tail = cache.getLeastRecentlyUsedKey();
            assertSame(head, tail);
            assertThat(head, is(new RawString("key1".getBytes(StandardCharsets.UTF_8))));
            assertThat(cache.getLruKeysInOrder(), is(Arrays.asList(raw("key1"))));
        }

        cache.remove(new RawString("key1".getBytes(StandardCharsets.UTF_8)));
        {
            RawString head = cache.getMostRecentlyUsedKey();
            RawString tail = cache.getLeastRecentlyUsedKey();
            assertNull(head);
            assertNull(tail);
            assertThat(cache.getLruKeysInOrder(), is(Collections.emptyList()));
        }

        cache.store(createEntryHandle("key1", "value1".getBytes(StandardCharsets.UTF_8)));
        cache.clear();
        {
            RawString head = cache.getMostRecentlyUsedKey();
            RawString tail = cache.getLeastRecentlyUsedKey();
            assertNull(head);
            assertNull(tail);
            assertThat(cache.getLruKeysInOrder(), is(Collections.emptyList()));
        }
    }

    @Test
    public void testEviction() {
        ClientSideLRUCache cache = new ClientSideLRUCache(10, null);

        byte[] payload = "value".getBytes(StandardCharsets.UTF_8);

        final long ttl = 10000;
        final long maxSize = payload.length;
        cache.setTtl(ttl);
        cache.setMaxSize(maxSize);

        long now = System.currentTimeMillis();
        long oldTime = now - ttl * 10L;

        storeEntry(cache, "key1", payload, oldTime);
        storeEntry(cache, "key2", payload, oldTime);
        storeEntry(cache, "key3", payload, now);
        storeEntry(cache, "key4", payload, now);

        // remove key1 and key2 for ttl, then key3 for size
        Collection<RawString> evicted = cache.performEviction();

        assertThat(evicted.size(), is(3));
        assertThat(cache.getAndRetain(new RawString("key4".getBytes(StandardCharsets.UTF_8))), is(notNullValue()));
        assertThat(cache.getAndRetain(new RawString("key3".getBytes(StandardCharsets.UTF_8))), is(nullValue()));
        assertThat(cache.getAndRetain(new RawString("key2".getBytes(StandardCharsets.UTF_8))), is(nullValue()));
        assertThat(cache.getAndRetain(new RawString("key1".getBytes(StandardCharsets.UTF_8))), is(nullValue()));
        assertThat(cache.getActualMemoryUsage(), is((long) payload.length));
    }

    private void storeEntry(ClientSideCache cache, String key, byte[] value, long forceLastGetTime) {
        EntryHandle entry1 = createEntryHandle(key, value);
        entry1.setLastGetTime(forceLastGetTime);
        cache.store(entry1);
    }

    private EntryHandle createEntryHandle(String key, byte[] data) {
        ByteBuf buffer = UnpooledByteBufAllocator.DEFAULT.heapBuffer(data.length);
        buffer.writeBytes(data);
        return new EntryHandle(new RawString(key.getBytes(StandardCharsets.UTF_8)), System.currentTimeMillis(), buffer, 0, key);
    }

    private RawString raw(String key) {
        return new RawString(key.getBytes(StandardCharsets.UTF_8));
    }
}
