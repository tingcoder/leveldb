package org.iq80.leveldb.log;

import lombok.extern.slf4j.Slf4j;
import org.iq80.leveldb.log.LogChunkType;
import org.iq80.leveldb.log.LogWriter;
import org.iq80.leveldb.log.Logs;
import org.iq80.leveldb.slice.Slice;
import org.iq80.leveldb.slice.SliceInput;
import org.iq80.leveldb.slice.SliceOutput;
import org.iq80.leveldb.slice.Slices;
import org.iq80.leveldb.util.ByteBufferSupport;
import org.iq80.leveldb.util.Closeables;

import javax.swing.*;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.iq80.leveldb.log.LogConstants.BLOCK_SIZE;
import static org.iq80.leveldb.log.LogConstants.HEADER_SIZE;

/**
 * @author
 */
@Slf4j
public class MMapLogWriter extends AbstractLogWriter {
    private static final int PAGE_SIZE = 1024 * 1024;

    private MappedByteBuffer mappedByteBuffer;
    private long fileOffset;

    public MMapLogWriter(File file, long fileNumber) throws IOException {
        requireNonNull(file, "file is null");
        checkArgument(fileNumber >= 0, "fileNumber is negative");
        this.file = file;
        this.fileNumber = fileNumber;
        this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
        mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0, PAGE_SIZE);
    }

    @Override
    public synchronized void close() throws IOException {
        log.info("MMapLogWriter {} 关闭", file.getName());
        closed.set(true);

        destroyMappedByteBuffer();

        if (fileChannel.isOpen()) {
            fileChannel.truncate(fileOffset);
        }

        // close the channel
        Closeables.closeQuietly(fileChannel);
    }

    private void destroyMappedByteBuffer() {
        if (mappedByteBuffer != null) {
            fileOffset += mappedByteBuffer.position();
            unmap();
        }
        mappedByteBuffer = null;
    }

    void writeChunk(LogChunkType type, Slice slice) throws IOException {
        checkArgument(slice.length() <= 0xffff, "length %s is larger than two bytes", slice.length());
        checkArgument(blockOffset + HEADER_SIZE <= BLOCK_SIZE);

        // create header
        Slice header = LogHeadUtils.newLogRecordHeader(type, slice);

        // write the header and the payload
        ensureCapacity(header.length() + slice.length());

        header.getBytes(0, mappedByteBuffer);
        slice.getBytes(0, mappedByteBuffer);

        blockOffset += HEADER_SIZE + slice.length();
    }

    @Override
    void fillRestBlock(int bytesRemainingInBlock) throws IOException {
        ensureCapacity(bytesRemainingInBlock);
        mappedByteBuffer.put(new byte[bytesRemainingInBlock]);
    }

    void ensureCapacity(int bytes) throws IOException {
        if (mappedByteBuffer.remaining() < bytes) {
            // remap
            fileOffset += mappedByteBuffer.position();
            unmap();
            mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, fileOffset, PAGE_SIZE);
        }
    }

    private void unmap() {
        ByteBufferSupport.unmap(mappedByteBuffer);
    }

    @Override
    void doForce() {
        mappedByteBuffer.force();
    }

}