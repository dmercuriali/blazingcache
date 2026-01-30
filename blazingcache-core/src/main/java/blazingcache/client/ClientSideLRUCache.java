package blazingcache.client;

import blazingcache.metrics.MetricsProvider;
import blazingcache.metrics.MonitoredAtomicLong;
import blazingcache.metrics.NullMetricsProvider;
import blazingcache.utils.RawString;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class ClientSideLRUCache implements ClientSideCache {
    private static final Logger LOGGER = Logger.getLogger(ClientSideLRUCache.class.getName());
    private final ConcurrentLinkedDeque<RawString> lruDeque;
    private final Map<RawString, EntryHandle> cache;
    private long maxSize;
    private long ttl;
    private final MonitoredAtomicLong actualMemory;
    private final AtomicLong oldestEvictedKeyAge;

    private ReadWriteLock evictionPolicyLock = new ReentrantReadWriteLock(true);
    private ReadWriteLock cacheLock = new ReentrantReadWriteLock(true);

    public ClientSideLRUCache(int expectedSize, MetricsProvider metricsProvider) {
        if (expectedSize <= 0) {
            expectedSize = 100;
        }
        MetricsProvider metricsProvider1 = metricsProvider == null ? NullMetricsProvider.INSTANCE : metricsProvider;

        this.lruDeque = new ConcurrentLinkedDeque<>();
        this.cache = new ConcurrentHashMap<>(expectedSize);
        this.actualMemory = new MonitoredAtomicLong(0L, metricsProvider1.getGaugeSet("blazingcache.client.memory.actualusage"));

        oldestEvictedKeyAge = new AtomicLong(0L);
    }

    private void moveToLRUHead(RawString key) {
        if (key == null) {
            return;
        }
        if (key.equals(lruDeque.peekFirst())) {
            return;
        }
        lruDeque.remove(key);
        lruDeque.addFirst(key);
    }

    private void removeFromLRU(RawString key) {
        if (key == null) {
            return;
        }
        lruDeque.remove(key);
    }

    private RawString pollLeastRecentlyUsedKey() {
        return lruDeque.pollLast();
    }

    @Override
    public long getMaxSize() {
        evictionPolicyLock.readLock().lock();
        try {
            return maxSize;
        } finally {
            evictionPolicyLock.readLock().unlock();
        }
    }

    @Override
    public void setMaxSize(long maxSize) {
        evictionPolicyLock.writeLock().lock();
        try {
            this.maxSize = maxSize;
        } finally {
            evictionPolicyLock.writeLock().unlock();
        }
    }

    @Override
    public long getTtl() {
        evictionPolicyLock.readLock().lock();
        try {
            return ttl;
        } finally {
            evictionPolicyLock.readLock().unlock();
        }
    }

    @Override
    public void setTtl(long ttl) {
        evictionPolicyLock.writeLock().lock();
        try {
            this.ttl = ttl;
        } finally {
            evictionPolicyLock.writeLock().unlock();
        }
    }

    @Override
    public EntryHandle getAndRetain(RawString key) {
        cacheLock.readLock().lock();
        try {
            EntryHandle entry = cache.computeIfPresent(key, (k, value) -> {
                value.retain();
                return value;
            });
            if (entry != null) {
                entry.setLastGetTime(System.nanoTime());
                moveToLRUHead(key);
            }
            return entry;
        } finally {
            cacheLock.readLock().unlock();
        }
    }

    @Override
    public void clear() {
        cacheLock.writeLock().lock();
        try {
            lruDeque.clear();
            Collection<RawString> keys = new ArrayList<>(this.cache.keySet());
            for (RawString k : keys) {
                remove(k);
            }
            actualMemory.reset();
        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    @Override
    public void store(EntryHandle entry) {
        if (entry == null) {
            throw new IllegalArgumentException("entry cannot be null");
        }

        RawString key = entry.getKey();

        cacheLock.writeLock().lock();
        try {
            if (cache.containsKey(key)) {
                //touch
                moveToLRUHead(key);
            } else {
                //insert
                lruDeque.addFirst(key);
            }

            cache.compute(key, (k, prev) -> {
                if (prev != null) {
                    actualMemory.addAndGet(-prev.getSerializedDataLength(), k);
                    prev.close();
                }
                return entry;
            });
            actualMemory.addAndGet(entry.getSerializedDataLength(), key);
        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    @Override
    public void remove(RawString key) {
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null");
        }

        cacheLock.writeLock().lock();
        try {
            removeFromLRU(key);

            cache.compute(key, (k, removed) -> {
                if (removed != null) {
                    actualMemory.addAndGet(-removed.getSerializedDataLength(), k);
                    removed.close();
                }
                // remove
                return null;
            });
        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    @Override
    public Collection<RawString> getByPrefix(RawString prefix) {
        cacheLock.readLock().lock();
        try {
            return cache.keySet().stream().filter(s -> s.startsWith(prefix)).collect(Collectors.toList());
        } finally {
            cacheLock.readLock().unlock();
        }
    }

    @Override
    public long getOldestEvictedKeyAge() {
        return oldestEvictedKeyAge.get();
    }

    @Override
    public int size() {
        return cache.size();
    }

    @Override
    public long getActualMemoryUsage() {
        return actualMemory.longValue();
    }

    @Override
    public Collection<RawString> performEviction() {
        cacheLock.writeLock().lock();
        try {
            Collection<RawString> ttlEvicted = performTtlEviction();
            Collection<RawString> sizeEvicted = performSizeBasedEviction();

            int size = sizeEvicted.size() + ttlEvicted.size();
            List<RawString> evicted = new ArrayList<>(size);
            evicted.addAll(ttlEvicted);
            evicted.addAll(sizeEvicted);
            return evicted;
        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    private Collection<RawString> performTtlEviction() {
        final long _ttl = getTtl();
        if (_ttl <= 0) {
            return Collections.emptyList();
        }

        RawString tail = getLeastRecentlyUsedKey();
        if (tail == null) {
            return Collections.emptyList();
        }

        boolean performTimeBasedEviction = false;
        final long now = System.currentTimeMillis();
        final long maxAge = now - _ttl;
        EntryHandle tailEntry = cache.get(tail);
        if (tailEntry != null) {
            // if the oldest entry is not evictable by ttl, then nothing will be
            long lastAccessTs = tailEntry.getLastGetTime();
            performTimeBasedEviction = lastAccessTs < maxAge;
        }

        if (!performTimeBasedEviction) {
            return Collections.emptyList();
        }

        LOGGER.log(Level.FINER, "evicting local entries before {0}", new Object[]{new java.util.Date(maxAge)});

        List<RawString> removedKeys = new ArrayList<>();
        long oldestEntryAge = 0;
        while (tail != null) {
            EntryHandle peekEntry = cache.get(tail);
            if (peekEntry != null && peekEntry.getLastGetTime() > maxAge) {
                break;
            }

            // remove from deque
            RawString _tail = pollLeastRecentlyUsedKey();
            if (_tail == null) {
                // something cleared the cache!
                break;
            }

            // remove from cache
            EntryHandle entry = cache.remove(tail);
            if (entry == null) {
                // something already removed the entry
                continue;
            }

            if (oldestEntryAge <= 0 || oldestEntryAge > entry.getPutTime()) {
                oldestEntryAge = entry.getPutTime();
            }

            actualMemory.addAndGet(-entry.getSerializedDataLength(), tail);
            entry.close();

            removedKeys.add(tail);

            if (LOGGER.isLoggable(Level.FINEST)) {
                LOGGER.log(Level.FINEST, "evict {0} size {1} bytes lastAccessDate {2}", new Object[]{entry.getKey(), entry.getSerializedDataLength(), entry.getLastGetTime()});
            }

            tail = getLeastRecentlyUsedKey();
        }

        this.oldestEvictedKeyAge.set(System.nanoTime() - oldestEntryAge);
        return removedKeys;
    }

    private Collection<RawString> performSizeBasedEviction() {
        final long _maxSize = getMaxSize();
        if (_maxSize <= 0) {
            return Collections.emptyList();
        }
        if (getLeastRecentlyUsedKey() == null) {
            return Collections.emptyList();
        }

        long bytesToEvict = actualMemory.longValue() - _maxSize;
        if (bytesToEvict <= 0) {
            return Collections.emptyList();
        }

        LOGGER.log(Level.FINER, "trying to release {0} bytes", new Object[]{bytesToEvict});

        List<RawString> removedKeys = new ArrayList<>();
        long oldestEntryAge = 0;
        while (bytesToEvict > 0) {
            // remove from deque
            RawString key = pollLeastRecentlyUsedKey();
            if (key == null) {
                break;
            }

            // remove from cache
            EntryHandle entry = cache.remove(key);
            if (entry == null) {
                continue;
            }

            if (oldestEntryAge <= 0 || oldestEntryAge > entry.getPutTime()) {
                oldestEntryAge = entry.getPutTime();
            }

            final long releasedMemory = entry.getSerializedDataLength();
            actualMemory.addAndGet(-releasedMemory, key);
            entry.close();

            removedKeys.add(key);

            if (LOGGER.isLoggable(Level.FINEST)) {
                LOGGER.log(Level.FINEST, "evict {0} size {1} bytes lastAccessDate {2}", new Object[]{entry.getKey(), entry.getSerializedDataLength(), entry.getLastGetTime()});
            }

            bytesToEvict -= releasedMemory;
        }

        this.oldestEvictedKeyAge.set(System.nanoTime() - oldestEntryAge);
        return removedKeys;
    }

    RawString getLeastRecentlyUsedKey() {
        return lruDeque.peekLast();
    }

    RawString getMostRecentlyUsedKey() {
        return lruDeque.peekFirst();
    }

    List<RawString> getLruKeysInOrder() {
        return new ArrayList<>(lruDeque);
    }
}
