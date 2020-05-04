import redis.clients.jedis.*;

import java.lang.ref.Cleaner;
import java.util.*;

public class RedisMap implements Map<String, String> {
    private static final Cleaner cleaner = Cleaner.create();

    private final Jedis repository;
    private final String repositoryIndex;
    private String connectionCountKey;

    private Set<Map.Entry<String, String>> entrySet;
    private Set<String> keySet;
    private Collection<String> values;

    public RedisMap() {
        this(Protocol.DEFAULT_HOST, Protocol.DEFAULT_PORT, 0, UUID.randomUUID().toString(), true);
    }

    public RedisMap(String repositoryIndex) {
        this(Protocol.DEFAULT_HOST, repositoryIndex);
    }

    public RedisMap(String host, String repositoryIndex) {
        this(host, Protocol.DEFAULT_PORT, repositoryIndex);
    }

    public RedisMap(String host, int port, String repositoryIndex) {
        this(host, port, 0, repositoryIndex);
    }

    public RedisMap(String host, int port, int dbIndex, String repositoryIndex) {
        this(host, port, dbIndex, repositoryIndex, false);
    }

    private RedisMap(String host, int port, int dbIndex, String repositoryIndex, boolean singleMode) {
        this.repository = new Jedis(host, port);
        repository.select(dbIndex);
        this.repositoryIndex = "rmap_" + repositoryIndex + "_repository";
        if (!singleMode) {
            this.connectionCountKey = "rmap_" + repositoryIndex + "_connectionCount";
            repository.incr(connectionCountKey);
        }
        cleaner.register(this, new PhantomRedisMap(this.repository,
                singleMode,
                this.connectionCountKey,
                this.repositoryIndex));
    }

    @Override
    public int size() {
        return repository.hlen(repositoryIndex).intValue();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        checkForNull(key);
        return repository.hexists(repositoryIndex, (String) key);
    }

    @Override
    public boolean containsValue(Object value) {
        checkForNull(value);
        return values().contains(value);
    }

    @Override
    public String get(Object key) {
        checkForNull(key);
        return repository.hget(repositoryIndex, (String) key);
    }

    @Override
    public String put(String key, String value) {
        checkForNull(key, value);
        Response<String> previousValue;
        try (Transaction transaction = repository.multi()) {
            previousValue = transaction.hget(repositoryIndex, key);
            transaction.hset(repositoryIndex, key, value);
            transaction.exec();
        }
        return previousValue == null ? null : previousValue.get();
    }

    @Override
    public String remove(Object key) {
        checkForNull(key);
        Response<String> removed;
        try (Transaction transaction = repository.multi()) {
            removed = transaction.hget(repositoryIndex, (String) key);
            transaction.hdel(repositoryIndex, (String) key);
            transaction.exec();
        }
        return removed == null ? null : removed.get();
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        checkForNull(m);
        repository.hset(repositoryIndex, (Map<String, String>) m);
    }

    @Override
    public void clear() {
        repository.del(repositoryIndex);
    }


    @Override
    public Set<String> keySet() {
        if (keySet == null) {
            keySet = new KeySet();
        }
        return keySet;
    }

    @Override
    public Collection<String> values() {
        if (values == null) {
            values = new Values();
        }
        return values;
    }

    @Override
    public Set<Map.Entry<String, String>> entrySet() {
        if (entrySet == null) {
            entrySet = new EntrySet();
        }
        return entrySet;
    }

    private void checkForNull(Object... value) {
        for (Object o : value) {
            Objects.requireNonNull(o);
            if (o instanceof Map) {
                ((Map<?, ?>) o).forEach((k, v) -> checkForNull(k, v));
            }
        }
    }

    private Set<Map.Entry<String, String>> getRedisEntrySet() {
        Set<Map.Entry<String, String>> result = new HashSet<>();
        ScanResult<Map.Entry<String, String>> scanResult;
        String startEndCursor = ScanParams.SCAN_POINTER_START;
        String cursor = startEndCursor;
        boolean cycleIsFinished = false;
        while (!cycleIsFinished) {
            scanResult = repository.hscan(repositoryIndex, cursor);
            result.addAll(scanResult.getResult());
            cursor = scanResult.getCursor();
            if (cursor.equals(startEndCursor)) {
                cycleIsFinished = true;
            }
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Map)) return false;
        Map<String, String> map = (Map<String, String>) o;
        return Objects.equals(entrySet(), map.entrySet());
    }

    @Override
    public int hashCode() {
        int hashCode = 0;
        for (Map.Entry<String, String> entry : entrySet()) {
            hashCode += entry.hashCode();
        }
        return hashCode;
    }

    abstract class BaseIterator {
        private final Iterator<Map.Entry<String, String>> iterator;
        private Map.Entry<String, String> current;

        BaseIterator() {
            this.iterator = getRedisEntrySet().iterator();
        }

        public boolean hasNext() {
            return iterator.hasNext();
        }

        public void remove() {
            iterator.remove();
            RedisMap.this.remove(current.getKey());
        }

        protected Map.Entry<String, String> nextEntry() {
            current = iterator.next();
            return current;
        }
    }

    class EntryIterator extends BaseIterator implements Iterator<Map.Entry<String, String>> {
        @Override
        public Map.Entry<String, String> next() {
            return new Entry(nextEntry());
        }
    }

    class KeyIterator extends BaseIterator implements Iterator<String> {
        @Override
        public String next() {
            return nextEntry().getKey();
        }
    }

    class ValueIterator extends BaseIterator implements Iterator<String> {
        @Override
        public String next() {
            return nextEntry().getValue();
        }
    }

    class Entry implements Map.Entry<String, String> {
        private final String key;
        private String value;

        Entry(Map.Entry<String, String> nextEntry) {
            this.key = nextEntry.getKey();
            this.value = nextEntry.getValue();
        }

        @Override
        public String getKey() {
            return this.key;
        }

        @Override
        public String getValue() {
            return this.value;
        }

        @Override
        public String setValue(String value) {
            this.value = value;
            return RedisMap.this.put(getKey(), value);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Map.Entry<String, String> entry = (Map.Entry<String, String>) o;
            return Objects.equals(key, entry.getKey()) &&
                    Objects.equals(value, entry.getValue());
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(key) ^ Objects.hashCode(value);
        }
    }

    class EntrySet extends AbstractSet<Map.Entry<String, String>> {
        @Override
        public Iterator<Map.Entry<String, String>> iterator() {
            return new EntryIterator();
        }

        @Override
        public int size() {
            return RedisMap.this.size();
        }

        @Override
        public boolean remove(Object o) {
            Map.Entry<String, String> entry = (Map.Entry<String, String>) o;
            return repository.hdel(repositoryIndex, entry.getKey()) != 0;
        }
    }

    class KeySet extends AbstractSet<String> {
        @Override
        public Iterator<String> iterator() {
            return new KeyIterator();
        }

        @Override
        public int size() {
            return RedisMap.this.size();
        }

        @Override
        public boolean remove(Object o) {
            return repository.hdel(repositoryIndex, (String) o) != 0;
        }
    }

    class Values extends AbstractCollection<String> {
        @Override
        public Iterator<String> iterator() {
            return new ValueIterator();
        }

        @Override
        public int size() {
            return RedisMap.this.size();
        }
    }

    static class PhantomRedisMap implements Runnable {
        private final Jedis repository;
        private final boolean singleMode;
        private final String connectionCountKey;
        private final String repositoryIndex;

        PhantomRedisMap(Jedis repository, boolean singleMode, String connectionCountKey, String repositoryIndex) {
            this.repository = repository;
            this.singleMode = singleMode;
            this.connectionCountKey = connectionCountKey;
            this.repositoryIndex = repositoryIndex;
        }

        @Override
        public void run() {
            try (repository) {
                if (singleMode) {
                    repository.del(repositoryIndex);
                } else {
                    repository.decr(connectionCountKey);
                    int count = Integer.parseInt(repository.get(connectionCountKey));
                    if (count <= 0) {
                        repository.del(connectionCountKey);
                        repository.del(repositoryIndex);
                    }
                }
            }
        }
    }
}
