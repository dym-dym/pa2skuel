package qengine.program;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Store extends HashMap<Integer, HashMap<Integer, List<Integer>>> {


    public Store() {
        super();
    }

    public void update(Integer key1, Integer key2, Integer key3) {
        this.computeIfAbsent(key1, key -> new HashMap<>()).computeIfAbsent(key2, key -> new ArrayList<>()).add(key3);
    }
}
