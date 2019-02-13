package org.iq80.leveldb.table;

import org.iq80.leveldb.DBComparator;
import org.iq80.leveldb.util.Slice;

/**
 * @author
 */
public class CustomUserComparator implements UserComparator {
    private final DBComparator comparator;

    public CustomUserComparator(DBComparator comparator) {
        this.comparator = comparator;
    }

    @Override
    public String name() {
        return comparator.name();
    }

    @Override
    public Slice findShortestSeparator(Slice start, Slice limit) {
        return new Slice(comparator.findShortestSeparator(start.getBytes(), limit.getBytes()));
    }

    @Override
    public Slice findShortSuccessor(Slice key) {
        return new Slice(comparator.findShortSuccessor(key.getBytes()));
    }

    @Override
    public int compare(Slice o1, Slice o2) {
        return comparator.compare(o1.getBytes(), o2.getBytes());
    }
}