package org.iq80.leveldb.util;

import org.iq80.leveldb.slice.Slice;

import java.util.Comparator;

/**
 * @author
 */
public final class SliceComparator implements Comparator<Slice> {

    public static final SliceComparator SLICE_COMPARATOR = new SliceComparator();

    @Override
    public int compare(Slice sliceA, Slice sliceB) {
        return sliceA.compareTo(sliceB);
    }
}