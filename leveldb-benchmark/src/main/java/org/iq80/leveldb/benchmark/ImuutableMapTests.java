package org.iq80.leveldb.benchmark;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.Ordering.natural;

public class ImuutableMapTests {
    public static void main(String[] args) {
        List<Long> datas1 = Lists.newArrayList(4L, 5L, 6L, 1L);
        List<Long> datas2 = Lists.newArrayList(3L, 5L, 6L, 2L);
        List<Long> datas3 = Lists.newArrayList(7L, 5L, 6L, 3L);
        List<Long> datas4 = Lists.newArrayList(1L, 5L, 9L, 2L);

        ImmutableMultimap.Builder<String, Long> builder = ImmutableMultimap.builder();
        builder = builder.orderKeysBy(natural());

        builder.putAll("datas1", datas1);
        builder.putAll("datas2", datas2);
        builder.putAll("datas3", datas3);
        builder.putAll("datas4", datas4);

        Multimap<String, Long> result = builder.build();
        Set<String> keySet = result.keySet();
        for (String key : keySet) {
            System.out.println("=============================================================== ");
            System.out.println( key +  " ==>> " + JSON.toJSONString(result.get(key)));
        }
    }
}
