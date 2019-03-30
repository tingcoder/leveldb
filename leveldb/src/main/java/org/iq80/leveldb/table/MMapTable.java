package org.iq80.leveldb.table;

import lombok.extern.slf4j.Slf4j;
import org.iq80.leveldb.slice.Slice;
import org.iq80.leveldb.slice.Slices;
import org.iq80.leveldb.util.ByteBufferSupport;
import org.iq80.leveldb.util.Closeables;
import org.iq80.leveldb.util.Snappy;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Comparator;
import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.checkArgument;
import static org.iq80.leveldb.CompressionType.SNAPPY;

@Slf4j
public class MMapTable extends Table {
    private MappedByteBuffer data;

    public MMapTable(String name, FileChannel fileChannel, Comparator<Slice> comparator, boolean verifyChecksums) throws IOException {
        super(name, fileChannel, comparator, verifyChecksums);
        checkArgument(fileChannel.size() <= Integer.MAX_VALUE, "File must be smaller than %s bytes", Integer.MAX_VALUE);
    }

    @Override
    protected Footer init() throws IOException {
        long size = fileChannel.size();
        data = fileChannel.map(MapMode.READ_ONLY, 0, size);
        Slice footerSlice = Slices.copiedBuffer(data, (int) size - Footer.ENCODED_LENGTH, Footer.ENCODED_LENGTH);
        return Footer.readFooter(footerSlice);
    }

    @Override
    public Callable<?> closer() {
        return new Closer(name, fileChannel, data);
    }

    private static class Closer implements Callable<Void> {
        private final String name;
        private final Closeable closeable;
        private final MappedByteBuffer data;

        public Closer(String name, Closeable closeable, MappedByteBuffer data) {
            this.name = name;
            this.closeable = closeable;
            this.data = data;
        }

        public Void call() {
            ByteBufferSupport.unmap(data);
            Closeables.closeQuietly(closeable);
            return null;
        }
    }

    @SuppressWarnings({"NonPrivateFieldAccessedInSynchronizedContext", "AssignmentToStaticFieldFromInstanceMethod"})
    @Override
    protected Block readBlock(BlockHandle blockHandle) throws IOException {
        // read block trailer
        BlockTrailer blockTrailer = BlockTrailer.readBlockTrailer(Slices.copiedBuffer(this.data, (int) blockHandle.getOffset() + blockHandle.getDataSize(), BlockTrailer.ENCODED_LENGTH));
        // decompress data
        Slice uncompressedData;
        ByteBuffer uncompressedBuffer = read((int) blockHandle.getOffset(), blockHandle.getDataSize());
        if (blockTrailer.getCompressionType() == SNAPPY) {
            synchronized (MMapTable.class) {
                int uncompressedLength = uncompressedLength(uncompressedBuffer);
                if (uncompressedScratch.capacity() < uncompressedLength) {
                    uncompressedScratch = ByteBuffer.allocateDirect(uncompressedLength);
                }
                uncompressedScratch.clear();

                Snappy.uncompress(uncompressedBuffer, uncompressedScratch);
                uncompressedData = Slices.copiedBuffer(uncompressedScratch);
            }
        } else {
            uncompressedData = Slices.copiedBuffer(uncompressedBuffer);
        }

        return new Block(uncompressedData, comparator);
    }

    private ByteBuffer read(int offset, int length) throws IOException {
        log.info("读取:{} 参数 offset: {} length:{}", name, offset, length);
        int newPosition = data.position() + offset;
        ByteBuffer block = (ByteBuffer) data.duplicate().order(ByteOrder.LITTLE_ENDIAN).clear().limit(newPosition + length).position(newPosition);
        return block;
    }
}