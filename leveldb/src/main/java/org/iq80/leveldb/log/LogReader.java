package org.iq80.leveldb.log;

import lombok.Getter;
import org.iq80.leveldb.slice.*;

import java.io.IOException;
import java.nio.channels.FileChannel;

import static org.iq80.leveldb.log.LogChunkType.*;
import static org.iq80.leveldb.log.LogConstants.BLOCK_SIZE;
import static org.iq80.leveldb.log.LogConstants.HEADER_SIZE;
import static org.iq80.leveldb.log.Logs.getChunkChecksum;

/**
 * 日志文件的reader工具
 *
 * @author yf
 */
public class LogReader {
    private final FileChannel fileChannel;

    private final LogMonitor monitor;

    private final boolean verifyChecksums;

    /**
     * Offset at which to start looking for the first record to return
     */
    private final long initialOffset;

    /**
     * Have we read to the end of the file?
     */
    private boolean eof;

    /**
     * Offset of the last record returned by readRecord.
     */
    @Getter
    private long lastRecordOffset;

    /**
     * Offset of the first location past the end of buffer.
     */
    private long endOfBufferOffset;

    /**
     * Scratch buffer in which the next record is assembled.
     */
    private final DynamicSliceOutput recordScratch = new DynamicSliceOutput(BLOCK_SIZE);

    /**
     * Scratch buffer for current block.  The currentBlock is sliced off the underlying buffer.
     */
    private final SliceOutput blockScratch = Slices.allocate(BLOCK_SIZE).output();

    /**
     * The current block records are being read from.
     */
    private SliceInput currentBlock = Slices.EMPTY_SLICE.input();

    /**
     * Current chunk which is sliced from the current block.
     */
    private Slice currentChunk = Slices.EMPTY_SLICE;

    public LogReader(FileChannel fileChannel, LogMonitor monitor, boolean verifyChecksums, long initialOffset) {
        this.fileChannel = fileChannel;
        this.monitor = monitor;
        this.verifyChecksums = verifyChecksums;
        this.initialOffset = initialOffset;
    }

    /**
     * Skips all blocks that are completely before "initial_offset_".
     * <p/>
     * Handles reporting corruption
     *
     * @return true on success.
     */
    private boolean skipToInitialBlock() {
        int offsetInBlock = (int) (initialOffset % BLOCK_SIZE);
        long blockStartLocation = initialOffset - offsetInBlock;

        // Don't search a block if we'd be in the trailer
        if (offsetInBlock > BLOCK_SIZE - 6) {
            blockStartLocation += BLOCK_SIZE;
        }

        endOfBufferOffset = blockStartLocation;

        // Skip to start of first block that can contain the initial record
        if (blockStartLocation > 0) {
            try {
                fileChannel.position(blockStartLocation);
            } catch (IOException e) {
                reportDrop(blockStartLocation, e);
                return false;
            }
        }

        return true;
    }

    public Slice readRecord() {
        recordScratch.reset();

        // advance to the first record, if we haven't already
        if (lastRecordOffset < initialOffset) {
            if (!skipToInitialBlock()) {
                return null;
            }
        }

        // Record offset of the logical record that we're reading
        long prospectiveRecordOffset = 0;

        boolean inFragmentedRecord = false;
        while (true) {
            long physicalRecordOffset = endOfBufferOffset - currentChunk.length();
            LogChunkType chunkType = readNextChunk();
            switch (chunkType) {
                case FULL:
                    if (inFragmentedRecord) {
                        reportCorruption(recordScratch.size(), "Partial record without end");
                        // simply return this full block
                    }
                    recordScratch.reset();
                    prospectiveRecordOffset = physicalRecordOffset;
                    lastRecordOffset = prospectiveRecordOffset;
                    return currentChunk.copySlice();

                case FIRST:
                    if (inFragmentedRecord) {
                        //当前已经是局部chunk说明文件格式有误
                        reportCorruption(recordScratch.size(), "Partial record without end");
                        //清空缓冲区
                        recordScratch.reset();
                    }
                    prospectiveRecordOffset = physicalRecordOffset;

                    //将当前chunk写入缓冲区
                    recordScratch.writeBytes(currentChunk);

                    //标记为局部chunk
                    inFragmentedRecord = true;
                    break;

                case MIDDLE:
                    if (!inFragmentedRecord) {
                        //当前不是局部chunk说明文件格式有问题
                        reportCorruption(recordScratch.size(), "Missing start of fragmented record");
                        //清空缓冲区
                        recordScratch.reset();
                    } else {
                        //将当前chunk追加到缓冲区
                        recordScratch.writeBytes(currentChunk);
                    }
                    break;

                case LAST:
                    if (!inFragmentedRecord) {
                        //当前不是局部chunk说明文件格式有问题
                        reportCorruption(recordScratch.size(), "Missing start of fragmented record");
                        //清空缓冲区
                        recordScratch.reset();
                    } else {
                        //将当前chunk追加到缓冲区
                        recordScratch.writeBytes(currentChunk);
                        lastRecordOffset = prospectiveRecordOffset;

                        //返回结果
                        return recordScratch.slice().copySlice();
                    }
                    break;

                case EOF:
                    if (inFragmentedRecord) {
                        //当前已经是局部chunk说明文件格式有误
                        reportCorruption(recordScratch.size(), "Partial record without end");
                        // clear the scratch and return
                        recordScratch.reset();
                    }
                    return null;

                case BAD_CHUNK:
                    if (inFragmentedRecord) {
                        //当前已经是局部chunk说明文件格式有误
                        reportCorruption(recordScratch.size(), "Error in middle of record");
                        inFragmentedRecord = false;
                        recordScratch.reset();
                    }
                    break;

                default:
                    //遇到"UNKNOWN"类型的chunk
                    int dropSize = currentChunk.length();
                    if (inFragmentedRecord) {
                        dropSize += recordScratch.size();
                    }
                    reportCorruption(dropSize, String.format("Unexpected chunk type %s", chunkType));
                    //重置局部chunk标记
                    inFragmentedRecord = false;
                    //清空缓冲区
                    recordScratch.reset();
                    break;
            }
        }
    }

    /**
     * Return type, or one of the preceding special values
     */
    private LogChunkType readNextChunk() {
        //清理当前chunk
        currentChunk = Slices.EMPTY_SLICE;

        //若当前block不足HEADER_SIZE,读取下一个block
        if (currentBlock.available() < HEADER_SIZE) {
            if (!readNextBlock()) {
                if (eof) {
                    return EOF;
                }
            }
        }

        //解析chunk的头协议
        int expectedChecksum = currentBlock.readInt();
        int length = currentBlock.readUnsignedByte();
        length = length | currentBlock.readUnsignedByte() << 8;
        byte chunkTypeId = currentBlock.readByte();
        LogChunkType chunkType = getLogChunkTypeByPersistentId(chunkTypeId);

        //block剩余长度校验
        if (length > currentBlock.available()) {
            int dropSize = currentBlock.available() + HEADER_SIZE;
            reportCorruption(dropSize, "Invalid chunk length");
            currentBlock = Slices.EMPTY_SLICE.input();
            return BAD_CHUNK;
        }

        //跳过ZERO_TYPE类型的chunk
        if (chunkType == ZERO_TYPE && length == 0) {
            // Skip zero length record without reporting any drops since
            // such records are produced by the writing code.
            currentBlock = Slices.EMPTY_SLICE.input();
            return BAD_CHUNK;
        }

        // Skip physical record that started before initialOffset
        if (endOfBufferOffset - HEADER_SIZE - length < initialOffset) {
            currentBlock.skipBytes(length);
            return BAD_CHUNK;
        }

        //读取当前chunk
        currentChunk = currentBlock.readBytes(length);

        if (verifyChecksums) {
            // crc32校验
            int actualChecksum = getChunkChecksum(chunkTypeId, currentChunk);
            if (actualChecksum != expectedChecksum) {
                // Drop the rest of the buffer since "length" itself may have
                // been corrupted and if we trust it, we could find some
                // fragment of a real log record that just happens to look
                // like a valid log record.
                int dropSize = currentBlock.available() + HEADER_SIZE;
                currentBlock = Slices.EMPTY_SLICE.input();
                reportCorruption(dropSize, "Invalid chunk checksum");
                return BAD_CHUNK;
            }
        }

        // Skip unknown chunk types
        // Since this comes last so we the, know it is a valid chunk, and is just a type we don't understand
        if (chunkType == UNKNOWN) {
            reportCorruption(length, String.format("Unknown chunk type %d", chunkType.getPersistentId()));
            return BAD_CHUNK;
        }

        return chunkType;
    }

    /**
     * 读取当前block
     *
     * @return
     */
    private boolean readNextBlock() {
        if (eof) {
            return false;
        }

        // clear the block
        blockScratch.reset();

        // 读取一个完整的block
        while (blockScratch.writableBytes() > 0) {
            try {
                int bytesRead = blockScratch.writeBytes(fileChannel, blockScratch.writableBytes());
                if (bytesRead < 0) {
                    // no more bytes to read
                    eof = true;
                    break;
                }
                endOfBufferOffset += bytesRead;
            } catch (IOException e) {
                currentBlock = Slices.EMPTY_SLICE.input();
                reportDrop(BLOCK_SIZE, e);
                eof = true;
                return false;
            }
        }

        //填充当前block
        currentBlock = blockScratch.slice().input();
        return currentBlock.isReadable();
    }

    /**
     * Reports corruption to the monitor.
     * The buffer must be updated to remove the dropped bytes prior to invocation.
     */
    private void reportCorruption(long bytes, String reason) {
        if (monitor != null) {
            monitor.corruption(bytes, reason);
        }
    }

    /**
     * Reports dropped bytes to the monitor.
     * The buffer must be updated to remove the dropped bytes prior to invocation.
     */
    private void reportDrop(long bytes, Throwable reason) {
        if (monitor != null) {
            monitor.corruption(bytes, reason);
        }
    }
}