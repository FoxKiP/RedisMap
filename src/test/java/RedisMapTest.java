import org.junit.After;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static org.junit.Assert.*;

public class RedisMapTest {
    private final Map<String, String> redisMap = new RedisMap();
    private final String key = "key";
    private final String value = "value";
    private final String newKey = "newKey";
    private final String newValue = "newValue";

    @After
    public void clearMap() {
        redisMap.clear();
    }

    @Test
    public void size() {
        assertEquals(0, redisMap.size());
        redisMap.put(key, value);
        assertEquals(1, redisMap.size());
    }

    @Test
    public void isEmpty() {
        assertTrue(redisMap.isEmpty());
    }

    @Test
    public void containsKey() {
        redisMap.put(key, value);
        assertTrue(redisMap.containsKey(key));
    }

    @Test
    public void containsValue() {
        redisMap.put(key, value);
        assertTrue(redisMap.containsValue(value));
    }

    @Test
    public void get() {
        assertNull(redisMap.get(key));
        redisMap.put(key, value);
        assertEquals(value, redisMap.get(key));
    }

    @Test
    public void put() {
        assertNull(redisMap.put(key, value));
        assertEquals(value, redisMap.get(key));
        assertEquals(value, redisMap.put(key, newValue));
    }

    @Test(expected = NullPointerException.class)
    public void putNullValue() {
        redisMap.put(key, null);
    }

    @Test
    public void remove() {
        assertNull(redisMap.remove(key));
        redisMap.put(key, value);
        assertEquals(value, redisMap.get(key));
        assertEquals(value, redisMap.remove(key));
        assertNotEquals(value, redisMap.get(key));

        redisMap.put(key, value);
        assertEquals(value, redisMap.get(key));
        redisMap.remove(key, newValue);
        assertEquals(value, redisMap.get(key));
        redisMap.remove(key, value);
        assertNotEquals(value, redisMap.get(key));
    }

    @Test
    public void putAll() {
        Map<String, String> map = Map.of(key, value);
        redisMap.putAll(map);
        assertEquals(value, redisMap.get(key));
    }

    @Test(expected = NullPointerException.class)
    public void putAllNullValue() {
        Map<String, String> map = new HashMap<>();
        map.put(key, null);
        redisMap.putAll(map);
    }

    @Test
    public void clear() {
        redisMap.put(key, value);
        assertEquals(value, redisMap.get(key));
        redisMap.clear();
        assertNotEquals(value, redisMap.get(key));
        assertTrue(redisMap.isEmpty());
    }

    @Test
    public void keySet() {
        redisMap.put(key, value);
        Set<String> keySet = redisMap.keySet();
        assertEquals(1, redisMap.size());
        assertEquals(1, keySet.size());
        assertTrue(keySet.contains(key));
        keySet.remove(key);
        assertEquals(0, redisMap.size());
        assertEquals(0, keySet.size());
        assertFalse(redisMap.containsKey(key));
    }

    @Test
    public void values() {
        redisMap.put(key, value);
        Collection<String> values = redisMap.values();
        assertEquals(1, redisMap.size());
        assertEquals(1, values.size());
        assertTrue(values.contains(value));
        values.remove(value);
        assertEquals(0, redisMap.size());
        assertEquals(0, values.size());
        assertNotEquals(value, redisMap.get(key));
    }

    @Test
    public void entrySet() {
        redisMap.put(key, value);
        Set<Map.Entry<String, String>> entrySet = redisMap.entrySet();
        assertEquals(1, redisMap.size());
        assertEquals(1, entrySet.size());
        Map.Entry<String, String> entry = entrySet.iterator().next();
        assertTrue(key.equals(entry.getKey()) && value.equals(entry.getValue()));
        entrySet.remove(entry);
        assertEquals(0, redisMap.size());
        assertEquals(0, entrySet.size());
        assertNotEquals(value, redisMap.get(key));
    }

    private final BiFunction<String, String, String> function = (k, v) -> newValue;

    @Test
    public void compute() {
        redisMap.put(key, value);
        redisMap.compute(key, function);
        assertEquals(newValue, redisMap.get(key));
    }

    @Test
    public void merge() {
        String returned = redisMap.merge(key, value, function);
        assertEquals(value, returned);
        assertEquals(value, redisMap.get(key));
        returned = redisMap.merge(key, value, function);
        assertEquals(newValue, redisMap.get(key));
        assertEquals(newValue, returned);
    }

    @Test
    public void computeIfPresent() {
        String returned = redisMap.computeIfPresent(key, function);
        assertFalse(redisMap.containsKey(key));
        assertNull(returned);
        redisMap.put(key, value);
        returned = redisMap.computeIfPresent(key, function);
        assertEquals(newValue, redisMap.get(key));
        assertEquals(newValue, returned);
    }

    @Test
    public void computeIfAbsent() {
        redisMap.put(key, value);
        String returned = redisMap.computeIfAbsent(key, k -> k + value);
        assertNotEquals(key + value, redisMap.get(key));
        assertEquals(value, returned);
        redisMap.clear();
        returned = redisMap.computeIfAbsent(key, k -> k + value);
        assertEquals(key + value, redisMap.get(key));
        assertEquals(key + value, returned);
    }

    @Test
    public void putIfAbsent() {
        assertNull(redisMap.putIfAbsent(key, value));
        assertEquals(value, redisMap.get(key));
        assertEquals(value, redisMap.putIfAbsent(key, newValue));
        assertEquals(value, redisMap.get(key));
    }

    @Test
    public void replace() {
        assertNull(redisMap.replace(key, value));
        assertNotEquals(value, redisMap.get(key));
        redisMap.put(key, value);
        assertEquals(value, redisMap.replace(key, newValue));
        assertEquals(newValue, redisMap.get(key));
        assertFalse(redisMap.replace(key, value, newValue));
        assertTrue(redisMap.replace(key, newValue, value));
        assertEquals(value, redisMap.get(key));
    }

    @Test
    public void equalsTest() {
        Map<String, String> redisMapTwo = new RedisMap();
        redisMap.put(key, value);
        redisMapTwo.put(key, value);
        assertEquals(redisMap, redisMapTwo);
        redisMapTwo.clear();
    }

    @Test
    public void hashCodeTest() {
        Map<String, String> redisMapTwo = new RedisMap();
        redisMap.put(key, value);
        redisMapTwo.put(key, value);
        assertEquals(redisMap.hashCode(), redisMapTwo.hashCode());
        redisMapTwo.clear();
    }

    @Test
    public void singleMap() {
        Map<String, String> redisMapTwo = new RedisMap();
        redisMap.put(key, value);
        redisMapTwo.put(newKey, newValue);
        assertFalse(redisMap.containsKey(newKey));
        assertFalse(redisMapTwo.containsKey(key));
        assertNotEquals(newValue, redisMap.get(newKey));
        assertNotEquals(value, redisMapTwo.get(key));
        redisMapTwo.clear();
    }

    @Test
    public void multiMap() {
        Map<String, String> redisMapOne = new RedisMap("test");
        Map<String, String> redisMapTwo = new RedisMap("test");
        redisMapOne.put(key, value);
        redisMapTwo.put(newKey, newValue);
        assertTrue(redisMapOne.containsKey(newKey));
        assertTrue(redisMapTwo.containsKey(key));
        assertEquals(newValue, redisMapOne.get(newKey));
        assertEquals(value, redisMapTwo.get(key));
        redisMapOne.clear();
        redisMapTwo.clear();
    }
}
