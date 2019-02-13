package org.iq80.leveldb.benchmark;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/***
 * @author yfeng
 * @date 2019-02-10 11:53
 */
public class ConcurrentSkipListMapTest {
    public static void main(String[] args) {
        ConcurrentSkipListMapTest tc = new ConcurrentSkipListMapTest();
        tc.queryTest();
    }

    public void queryTest() {
        ConcurrentSkipListMap<String, Long> map = new ConcurrentSkipListMap();
        map.put("user-10", 10L);
        map.put("user-21", 21L);
        map.put("user-13", 13L);
        map.put("user-34", 34L);

        Long u11Val = map.get("user-11");
        Long u10Val = map.get("user-10");
        System.out.println(u11Val);
        System.out.println(u10Val);
        Map.Entry<String, Long> u20Entry = map.ceilingEntry("user-20");
        System.out.println(u20Entry.getKey() + " <--> " + u20Entry.getValue());

        System.out.println("----------------------------------");
        map.entrySet().forEach((entry)->{
            System.out.println(entry.getKey() + " <--> " + entry.getValue());
        });
        System.out.println("----------------------------------");
        map.remove("user-13");
        map.entrySet().forEach((entry)->{
            System.out.println(entry.getKey() + " <--> " + entry.getValue());
        });
    }
}
