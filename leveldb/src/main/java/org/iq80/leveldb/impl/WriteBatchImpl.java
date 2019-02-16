package org.iq80.leveldb.impl;

import com.google.common.collect.Maps;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.slice.Slice;
import org.iq80.leveldb.slice.Slices;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import static java.util.Objects.requireNonNull;

/**
 * @author
 */
public class WriteBatchImpl implements WriteBatch {
    private final List<Entry<Slice, Slice>> batch = new ArrayList<>();
    private int approximateSize;

    public int getApproximateSize() {
        return approximateSize;
    }

    public int size() {
        return batch.size();
    }

    @Override
    public WriteBatchImpl put(byte[] key, byte[] value) {
        requireNonNull(key, "key is null");
        requireNonNull(value, "value is null");
        batch.add(Maps.immutableEntry(Slices.wrappedBuffer(key), Slices.wrappedBuffer(value)));
        approximateSize += 12 + key.length + value.length;
        return this;
    }

    public WriteBatchImpl put(Slice key, Slice value) {
        requireNonNull(key, "key is null");
        requireNonNull(value, "value is null");
        batch.add(Maps.immutableEntry(key, value));
        approximateSize += 12 + key.length() + value.length();
        return this;
    }

    @Override
    public WriteBatchImpl delete(byte[] key) {
        requireNonNull(key, "key is null");
        batch.add(Maps.immutableEntry(Slices.wrappedBuffer(key), (Slice) null));
        approximateSize += 6 + key.length;
        return this;
    }

    public WriteBatchImpl delete(Slice key) {
        requireNonNull(key, "key is null");
        batch.add(Maps.immutableEntry(key, (Slice) null));
        approximateSize += 6 + key.length();
        return this;
    }

    @Override
    public void close() {
    }

    public void forEach(Handler handler) {
        for (Entry<Slice, Slice> entry : batch) {
            Slice key = entry.getKey();
            Slice value = entry.getValue();
            if (value != null) {
                handler.put(key, value);
            } else {
                handler.delete(key);
            }
        }
    }

    public interface Handler {
        void put(Slice key, Slice value);

        void delete(Slice key);
    }
}
