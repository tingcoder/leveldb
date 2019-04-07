package org.iq80.leveldb.log;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.iq80.leveldb.slice.Slice;
import org.iq80.leveldb.slice.SliceInput;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;
import static org.iq80.leveldb.log.LogConstants.BLOCK_SIZE;
import static org.iq80.leveldb.log.LogConstants.HEADER_SIZE;

@Slf4j
public abstract class AbstractLogWriter implements LogWriter {

    protected final AtomicBoolean closed = new AtomicBoolean();

    @Getter
    protected File file;
    protected FileChannel fileChannel;
    @Getter
    protected long fileNumber;
    /**
     * Current offset in the current block
     */
    protected int blockOffset;

    @Override
    public synchronized void addRecord(Slice record, boolean force) throws IOException {
        checkState(!closed.get(), "Log has been closed");

        SliceInput sliceInput = record.input();

        // used to track first, middle and last blocks
        boolean begin = true;

        // Fragment the record int chunks as necessary and write it.  Note that if record
        // is empty, we still want to iterate once to write a single
        // zero-length chunk.
        do {
            int bytesRemainingInBlock = BLOCK_SIZE - blockOffset;
            checkState(bytesRemainingInBlock >= 0);

            // Switch to a new block if necessary
            if (bytesRemainingInBlock < HEADER_SIZE) {
                if (bytesRemainingInBlock > 0) {
                    // Fill the rest of the block with zeros
                    // todo lame... need a better way to write zeros
                    fillRestBlock(bytesRemainingInBlock);
                }
                blockOffset = 0;
                bytesRemainingInBlock = BLOCK_SIZE - blockOffset;
            }

            // Invariant: we never leave less than HEADER_SIZE bytes available in a block
            int bytesAvailableInBlock = bytesRemainingInBlock - HEADER_SIZE;
            checkState(bytesAvailableInBlock >= 0);

            // if there are more bytes in the record then there are available in the block,
            // fragment the record; otherwise write to the end of the record
            boolean end;
            int fragmentLength;
            if (sliceInput.available() > bytesAvailableInBlock) {
                end = false;
                fragmentLength = bytesAvailableInBlock;
            } else {
                end = true;
                fragmentLength = sliceInput.available();
            }

            // determine block type
            LogChunkType type;
            if (begin && end) {
                type = LogChunkType.FULL;
            } else if (begin) {
                type = LogChunkType.FIRST;
            } else if (end) {
                type = LogChunkType.LAST;
            } else {
                type = LogChunkType.MIDDLE;
            }

            // write the chunk
            Slice writeData = sliceInput.readBytes(fragmentLength);

            log.info("将数据:{} chunkType: {}  写入日志{}文件", writeData, type.name(), file.getName());
            writeChunk(type, writeData);

            // we are no longer on the first chunk
            begin = false;
        } while (sliceInput.isReadable());

        //是否立即刷盘
        log.info("强刷磁盘: {}", force);
        if (force) {
            doForce();
        }
    }

    abstract void ensureCapacity(int bytesLengh) throws IOException;

    abstract void doForce() throws IOException;

    abstract void writeChunk(LogChunkType type, Slice slice) throws IOException;

    abstract void fillRestBlock(int bytesRemainingInBlock) throws IOException;

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public synchronized void delete() throws IOException {
        close();
        // try to delete the file
        file.delete();
    }

}