package org.iq80.leveldb.log;

import lombok.extern.slf4j.Slf4j;
import org.iq80.leveldb.impl.Iq80DBFactory;
import org.iq80.leveldb.slice.Slice;
import org.iq80.leveldb.util.PureJavaCrc32C;

import java.io.File;
import java.io.IOException;

/**
 * @author
 */
@Slf4j
public final class Logs {

    public static LogWriter createLogWriter(File file, long fileNumber) throws IOException {
        if (Iq80DBFactory.USE_MMAP) {
            log.info("使用MMapLogWriter开启{}的日志写入工具", file.getName());
            return new MMapLogWriter(file, fileNumber);
        } else {
            log.info("使用FileChannelLogWriter开启{}的日志写入工具", file.getName());
            return new FileChannelLogWriter(file, fileNumber);
        }
    }

    public static int getChunkChecksum(int chunkTypeId, Slice slice) {
        return getChunkChecksum(chunkTypeId, slice.getRawArray(), slice.getRawOffset(), slice.length());
    }

    public static int getChunkChecksum(int chunkTypeId, byte[] buffer, int offset, int length) {
        PureJavaCrc32C crc32C = new PureJavaCrc32C();
        crc32C.update(chunkTypeId);
        crc32C.update(buffer, offset, length);
        return crc32C.getMaskedValue();
    }
}
