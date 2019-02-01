package org.iq80.leveldb.impl;

import java.util.Map.Entry;

/**
 * @author
 * @param <K>
 * @param <V>
 */
public interface SeekingIterable<K, V> extends Iterable<Entry<K, V>> {
    /**
     * 返回遍历器
     * @return
     */
    @Override
    SeekingIterator<K, V> iterator();
}
