package qengine.program;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class Store extends HashMap<Integer, HashMap<Integer, List<Integer>>> {


    public Store() {
        super();
    }

    public void update(Integer key1, Integer key2, Integer key3) {
        this.computeIfAbsent(key1, key -> new HashMap<>()).computeIfAbsent(key2, key -> new ArrayList<>()).add(key3);
    }

    public  List<Integer> twoValuesFilter(Integer first, Integer second) {
        return this.entrySet().stream()
                .filter(entry -> entry.getKey().equals(first))
                .flatMap(entry -> entry.getValue().entrySet().stream())
                .filter(innerEntry -> innerEntry.getKey().equals(second))
                .flatMap(innerEntry -> innerEntry.getValue().stream())
                .collect(Collectors.toList());
    }
}
