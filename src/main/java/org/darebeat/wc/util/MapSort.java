package org.darebeat.wc.util;

import java.util.*;

/**
 * Created by darebeat on 10/16/16.
 */
public class MapSort {
    public static Map<String,Integer> sortByValue(Map<String,Integer> map){
        if (map == null) return null;

        List list = new LinkedList(map.entrySet());

        Collections.sort(list, new Comparator() {
            public int compare(Object o1, Object o2) {
                Comparable sort1 = (Comparable) ((Map.Entry) o1).getValue();
                Comparable sort2 = (Comparable) ((Map.Entry) o2).getValue();
                return sort2.compareTo(sort1);
            }
        });

        Map result = new LinkedHashMap();

        Iterator it = list.iterator();

        while (it.hasNext()){
            Map.Entry entry = (Map.Entry) it.next();
            result.put(entry.getKey(),entry.getValue());
        }

        return result;
    }
}
