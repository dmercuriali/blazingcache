package blazingcache.client;

import blazingcache.utils.RawString;
import java.util.Collection;

public interface ClientSideCache {
    long getMaxSize();

    void setMaxSize(long maxSize);

    long getTtl();

    void setTtl(long ttl);

    EntryHandle getAndRetain(RawString key);

    Collection<RawString> getByPrefix(RawString prefix);

    void store(EntryHandle entry);

    void remove(RawString key);

    void clear();

    Collection<RawString> performEviction();

    long getOldestEvictedKeyAge();

    int size();

    long getActualMemoryUsage();
}
