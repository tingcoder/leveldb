package org.iq80.leveldb.log;


import org.iq80.leveldb.slice.Slice;
import org.iq80.leveldb.slice.SliceInput;
import org.iq80.leveldb.slice.SliceOutput;
import org.iq80.leveldb.slice.Slices;
import org.iq80.leveldb.util.Closeables;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.iq80.leveldb.log.LogConstants.BLOCK_SIZE;
import static org.iq80.leveldb.log.LogConstants.HEADER_SIZE;

/**
 * @author
 */
public class FileChannelLogWriter extends AbstractLogWriter {
    public FileChannelLogWriter(File file, long fileNumber) throws FileNotFoundException {
        requireNonNull(file, "file is null");
        checkArgument(fileNumber >= 0, "fileNumber is negative");

        this.file = file;
        this.fileNumber = fileNumber;
        this.fileChannel = new FileOutputStream(file).getChannel();
    }


    @Override
    public synchronized void close() {
        closed.set(true);

        // try to forces the log to disk
        try {
            fileChannel.force(true);
        } catch (IOException ignored) {
        }

        // close the channel
        Closeables.closeQuietly(fileChannel);
    }

    @Override
    void ensureCapacity(int bytes) throws IOException {

    }

    @Override
    void doForce() throws IOException {
        fileChannel.force(false);
    }

    void writeChunk(LogChunkType type, Slice slice) throws IOException {
        checkArgument(slice.length() <= 0xffff, "length %s is larger than two bytes", slice.length());
        checkArgument(blockOffset + HEADER_SIZE <= BLOCK_SIZE);

        // create header
        Slice header = LogHeadUtils.newLogRecordHeader(type, slice);

        // write the header and the payload
        header.getBytes(0, fileChannel, header.length());
        slice.getBytes(0, fileChannel, slice.length());

        blockOffset += HEADER_SIZE + slice.length();
    }

    @Override
    void fillRestBlock(int bytesRemainingInBlock) throws IOException {
        fileChannel.write(ByteBuffer.allocate(bytesRemainingInBlock));
    }
}