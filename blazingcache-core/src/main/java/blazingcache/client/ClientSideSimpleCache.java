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
package blazingcache.client;

import blazingcache.metrics.MetricsProvider;
import blazingcache.metrics.MonitoredAtomicLong;
import blazingcache.metrics.NullMetricsProvider;
import blazingcache.utils.RawString;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * This is a simple implementation of a client side cache
 *
 * @author dennis.mercuriali
 */
public class ClientSideSimpleCache implements ClientSideCache {

    private static final Logger LOGGER = Logger.getLogger(ClientSideSimpleCache.class.getName());
    private final ConcurrentHashMap<RawString, EntryHandle> cache;
    private volatile long maxSize;
    private volatile long ttl;
    private long lastPerformedEvictionTimestamp;
    private final MonitoredAtomicLong actualMemory;
    private final AtomicLong oldestEvictedKeyAge;

    /**
     * Creates a new ClientSideSimpleCache with an empty cache.
     */
    public ClientSideSimpleCache(MetricsProvider metricsProvider) {
        this.cache = new ConcurrentHashMap<>();

        MetricsProvider metricsProvider1 = metricsProvider == null ? NullMetricsProvider.INSTANCE : metricsProvider;
        this.actualMemory = new MonitoredAtomicLong(0L, metricsProvider1.getGaugeSet("blazingcache.client.memory.actualusage"));

        this.oldestEvictedKeyAge = new AtomicLong(0L);
    }

    @Override
    public long getMaxSize() {
        return maxSize;
    }

    @Override
    public void setMaxSize(long maxSize) {
        this.maxSize = maxSize;
    }

    @Override
    public long getTtl() {
        return ttl;
    }

    @Override
    public void setTtl(long ttl) {
        this.ttl = ttl;
    }

    @Override
    public EntryHandle getAndRetain(RawString key) {
        EntryHandle entry = cache.computeIfPresent(key, (k, value) -> {
            value.retain();
            return value;
        });

        if (entry != null) {
            entry.setLastGetTime(System.nanoTime());
        }
        return entry;
    }

    @Override
    public Collection<RawString> getByPrefix(RawString prefix) {
        return cache.keySet().stream()
                .filter(s -> s.startsWith(prefix))
                .collect(Collectors.toList());
    }

    @Override
    public void clear() {
        Collection<RawString> keys = new ArrayList<>(this.cache.keySet());
        for (RawString k : keys) {
            remove(k);
        }
    }

    @Override
    public void store(EntryHandle entry) {
        RawString _key = entry.getKey();
        cache.compute(_key, (k, prev) -> {
            if (prev != null) {
                actualMemory.addAndGet(-prev.getSerializedDataLength(), k);
                prev.close();
            }
            return entry;
        });
        actualMemory.addAndGet(entry.getSerializedDataLength(), _key);
    }

    @Override
    public void remove(RawString key) {
        cache.compute(key, (k, removed) -> {
            if (removed != null) {
                actualMemory.addAndGet(-removed.getSerializedDataLength(), k);
                removed.close();
            }
            // remove
            return null;
        });
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
        final long _maxSize = getMaxSize();
        final long _ttl = getTtl();
        final long deltaMemory = _maxSize - actualMemory.longValue();
        final long now = System.currentTimeMillis();
        final boolean performMaxEntryAgeEviction = _ttl > 0 && now - lastPerformedEvictionTimestamp >= _ttl / 2;
        final boolean performSizeEviction = _maxSize > 0 && deltaMemory < 0;

        if (!performSizeEviction && !performMaxEntryAgeEviction) {
            return Collections.emptyList();
        }
        this.lastPerformedEvictionTimestamp = now;

        final long to_release = performSizeEviction ? -deltaMemory : 0;
        final long maxAgeTs = now - _ttl;

        if (_maxSize > 0 && _ttl > 0) {
            LOGGER.log(Level.FINER, "trying to release {0} bytes, and evicting local entries before {1}", new Object[]{to_release, new java.util.Date(maxAgeTs)});
        } else if (_maxSize > 0) {
            LOGGER.log(Level.FINER, "trying to release {0} bytes", new Object[]{to_release});
        } else if (_ttl > 0) {
            LOGGER.log(Level.FINER, "evicting local entries before {0}", new Object[]{new java.util.Date(maxAgeTs)});
        }

        List<RawString> evicted = new ArrayList<>();
        java.util.function.Consumer<EntryHandle> accumulator = new java.util.function.Consumer<EntryHandle>() {
            long releasedMemory = 0;

            @Override
            public void accept(EntryHandle t) {
                boolean tooOld = _ttl > 0 && t.getLastGetTime() < maxAgeTs;
                boolean releaseForSize = performSizeEviction && releasedMemory < to_release;
                if (tooOld || releaseForSize) {
                    cache.compute(t.getKey(), (k, removed) -> {
                        if (removed != null) {
                            evicted.add(k);

                            final long size = removed.getSerializedDataLength();
                            releasedMemory += size;
                            actualMemory.addAndGet(-size, k);
                            removed.close();
                        }
                        // remove
                        return null;
                    });
                }
            }
        };

        List<EntryHandle> entries = new ArrayList<>(cache.values());
        entries.sort((EntryHandle o1, EntryHandle o2) -> {
            long diff = o1.getLastGetTime() - o2.getLastGetTime();
            if (diff == 0) {
                return 0;
            }
            return diff > 0 ? 1 : -1;
        });
        entries.forEach(accumulator);

        return evicted;
    }

}
