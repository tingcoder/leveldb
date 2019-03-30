package org.iq80.leveldb.impl;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import lombok.extern.slf4j.Slf4j;
import org.iq80.leveldb.slice.Slice;
import org.iq80.leveldb.util.InternalIterator;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_LONG;

/**
 * @author
 */
@Slf4j
public class MemTable implements SeekingIterable<InternalKey, Slice> {
    private final ConcurrentSkipListMap<InternalKey, Slice> table;
    private final AtomicLong approximateMemoryUsage = new AtomicLong();

    public MemTable(InternalKeyComparator internalKeyComparator) {
        table = new ConcurrentSkipListMap<>(internalKeyComparator);
    }

    public boolean isEmpty() {
        return table.isEmpty();
    }

    public long approximateMemoryUsage() {
        return approximateMemoryUsage.get();
    }

    public void add(long sequenceNumber, ValueType valueType, Slice key, Slice value) {
        requireNonNull(valueType, "valueType is null");
        requireNonNull(key, "key is null");
        requireNonNull(valueType, "valueType is null");

        InternalKey internalKey = new InternalKey(key, sequenceNumber, valueType);
        table.put(internalKey, value);

        //缓冲区大小记录
        approximateMemoryUsage.addAndGet(key.length() + SIZE_OF_LONG + value.length());
    }

    public LookupResult get(LookupKey key) {
        requireNonNull(key, "key is null");

        InternalKey internalKey = key.getInternalKey();
        Entry<InternalKey, Slice> entry = table.ceilingEntry(internalKey);
        if (entry == null) {
            log.info("memTable 查找{} 返回null", key.toString());
            return null;
        }
        InternalKey entryKey = entry.getKey();
        if (entryKey.getUserKey().equals(key.getUserKey())) {
            if (entryKey.getValueType() == ValueType.DELETION) {
                log.info("memTable 查找{} 返回Deleted", key.toString());
                return LookupResult.deleted(key);
            } else {
                log.info("memTable 查找{} 返回OK", key.toString());
                return LookupResult.ok(key, entry.getValue());
            }
        }
        return null;
    }

    @Override
    public MemTableIterator iterator() {
        return new MemTableIterator();
    }

    public class MemTableIterator implements InternalIterator {
        private PeekingIterator<Entry<InternalKey, Slice>> iterator;

        public MemTableIterator() {
            iterator = Iterators.peekingIterator(table.entrySet().iterator());
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public void seekToFirst() {
            iterator = Iterators.peekingIterator(table.entrySet().iterator());
        }

        @Override
        public void seek(InternalKey targetKey) {
            iterator = Iterators.peekingIterator(table.tailMap(targetKey).entrySet().iterator());
        }

        @Override
        public InternalEntry peek() {
            Entry<InternalKey, Slice> entry = iterator.peek();
            return new InternalEntry(entry.getKey(), entry.getValue());
        }

        @Override
        public InternalEntry next() {
            Entry<InternalKey, Slice> entry = iterator.next();
            return new InternalEntry(entry.getKey(), entry.getValue());
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
