package org.iq80.leveldb.impl;

import org.iq80.leveldb.slice.Slice;
import org.iq80.leveldb.slice.SliceInput;
import org.iq80.leveldb.slice.SliceOutput;
import org.iq80.leveldb.slice.Slices;

import java.io.IOException;

import static org.iq80.leveldb.impl.ValueType.DELETION;
import static org.iq80.leveldb.impl.ValueType.VALUE;
import static org.iq80.leveldb.slice.Slices.readLengthPrefixedBytes;
import static org.iq80.leveldb.slice.Slices.writeLengthPrefixedBytes;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_INT;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_LONG;

public class WriteBatchUtils {

    public static WriteBatchImpl readWriteBatch(SliceInput record, int updateSize) throws IOException {
        WriteBatchImpl writeBatch = new WriteBatchImpl();
        int entries = 0;
        while (record.isReadable()) {
            entries++;
            ValueType valueType = ValueType.getValueTypeByPersistentId(record.readByte());
            if (valueType == VALUE) {
                Slice key = readLengthPrefixedBytes(record);
                Slice value = readLengthPrefixedBytes(record);
                writeBatch.put(key, value);
            } else if (valueType == DELETION) {
                Slice key = readLengthPrefixedBytes(record);
                writeBatch.delete(key);
            } else {
                throw new IllegalStateException("Unexpected value type " + valueType);
            }
        }
        if (entries != updateSize) {
            throw new IOException(String.format("Expected %d entries in logWriter record but found %s entries", updateSize, entries));
        }
        return writeBatch;
    }

    public static Slice writeWriteBatch(WriteBatchImpl updates, long sequenceBegin) {
        //step 1 : 计算容量，申请空间
        Slice record = Slices.allocate(SIZE_OF_LONG + SIZE_OF_INT + updates.getApproximateSize());

        //step 2 : 写入sequence和updateSize
        final SliceOutput sliceOutput = record.output();
        sliceOutput.writeLong(sequenceBegin);
        sliceOutput.writeInt(updates.size());

        //step 3 : 写入更新指令
        updates.forEach(new WriteBatchImpl.Handler() {
            @Override
            public void put(Slice key, Slice value) {
                sliceOutput.writeByte(VALUE.getPersistentId());
                writeLengthPrefixedBytes(sliceOutput, key);
                writeLengthPrefixedBytes(sliceOutput, value);
            }

            @Override
            public void delete(Slice key) {
                sliceOutput.writeByte(DELETION.getPersistentId());
                writeLengthPrefixedBytes(sliceOutput, key);
            }
        });

        //step 4 : 拷贝结果并返回
        return record.slice(0, sliceOutput.size());
    }
}
