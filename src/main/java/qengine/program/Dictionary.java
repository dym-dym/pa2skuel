package qengine.program;

import java.util.HashMap;

public class Dictionary<V> {

    private final HashMap<Integer, V> dictionary = new HashMap<Integer, V>();
    private final HashMap<V, Integer> invertedDictionary = new HashMap<V, Integer>();

    private Integer currentKey;

    public Dictionary() {
        currentKey = 0;
    }

    public Integer getKeyIfExists(V value) {
        if (getInvertedDictionary().containsKey(value)) {
            return getInvertedDictionary().get(value);
        }

        currentKey++;

        getDictionary().put(currentKey, value);
        getInvertedDictionary().put(value, currentKey);

        return currentKey;
    }

    public V getValue(Integer key) {
        return getDictionary().get(key);
    }

    public HashMap<Integer, V> getDictionary() {
        return dictionary;
    }

    public HashMap<V, Integer> getInvertedDictionary() {
        return invertedDictionary;
    }
}
