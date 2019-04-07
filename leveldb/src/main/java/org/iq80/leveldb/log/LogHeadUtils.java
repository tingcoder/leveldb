package org.iq80.leveldb.log;

import org.iq80.leveldb.slice.Slice;
import org.iq80.leveldb.slice.SliceOutput;
import org.iq80.leveldb.slice.Slices;

import static org.iq80.leveldb.log.LogConstants.HEADER_SIZE;

public class LogHeadUtils {

    /**
     * 构建一个Head头
     * 4位crc + 2位长度 + 1位类型
     *
     * @param type
     * @param slice
     * @return
     */
    public static Slice newLogRecordHeader(LogChunkType type, Slice slice) {
        int crc = Logs.getChunkChecksum(type.getPersistentId(), slice.getRawArray(), slice.getRawOffset(), slice.length());
        // Format the header
        Slice header = Slices.allocate(HEADER_SIZE);
        SliceOutput sliceOutput = header.output();
        sliceOutput.writeInt(crc);
        sliceOutput.writeByte((byte) (slice.length() & 0xff));
        sliceOutput.writeByte((byte) (slice.length() >>> 8));
        sliceOutput.writeByte((byte) (type.getPersistentId()));
        return header;
    }
}
