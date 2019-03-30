package org.iq80.leveldb.impl;

import com.google.common.cache.*;
import org.iq80.leveldb.slice.Slice;
import org.iq80.leveldb.table.FileChannelTable;
import org.iq80.leveldb.table.MMapTable;
import org.iq80.leveldb.table.Table;
import org.iq80.leveldb.table.UserComparator;
import org.iq80.leveldb.util.Finalizer;
import org.iq80.leveldb.util.InternalTableIterator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutionException;

import static java.util.Objects.requireNonNull;

/**
 * TableCache连接文件编号和SST表的关系
 *
 * @author
 */
public class TableCache {
    private final LoadingCache<Long, TableAndFile> cache;
    private final Finalizer<Table> finalizer = new Finalizer<>(1);

    public TableCache(final File databaseDir, int tableCacheSize, final UserComparator userComparator, final boolean verifyChecksums) {
        requireNonNull(databaseDir, "databaseName is null");

        cache = CacheBuilder.newBuilder()
                .maximumSize(tableCacheSize)
                .removalListener(new RemovalListener<Long, TableAndFile>() {
                    @Override
                    public void onRemoval(RemovalNotification<Long, TableAndFile> notification) {
                        Table table = notification.getValue().getTable();
                        finalizer.addCleanup(table, table.closer());
                    }
                })
                .build(new CacheLoader<Long, TableAndFile>() {
                    @Override
                    public TableAndFile load(Long fileNumber) throws IOException {
                        return new TableAndFile(databaseDir, fileNumber, userComparator, verifyChecksums);
                    }
                });
    }

    public InternalTableIterator newIterator(FileMetaData file) {
        return newIterator(file.getNumber());
    }

    public InternalTableIterator newIterator(long number) {
        return new InternalTableIterator(getTable(number).iterator());
    }

    public long getApproximateOffsetOf(FileMetaData file, Slice key) {
        return getTable(file.getNumber()).getApproximateOffsetOf(key);
    }

    private Table getTable(long number) {
        Table table;
        try {
            table = cache.get(number).getTable();
        } catch (ExecutionException e) {
            Throwable cause = e;
            if (e.getCause() != null) {
                cause = e.getCause();
            }
            throw new RuntimeException("Could not open table " + number, cause);
        }
        return table;
    }

    public void close() {
        cache.invalidateAll();
        finalizer.destroy();
    }

    public void evict(long number) {
        cache.invalidate(number);
    }

    private static final class TableAndFile {
        private final Table table;

        /**
         * 根据文件编号和目录返回 ${fileNumber}.sst为存储介质的数据表格
         *
         * @param databaseDir     目录
         * @param fileNumber      文件存储编号
         * @param userComparator  比较器
         * @param verifyCheckSums 是否做求和校验
         * @throws IOException
         */
        private TableAndFile(File databaseDir, long fileNumber, UserComparator userComparator, boolean verifyCheckSums) throws IOException {
            String tableFileName = Filename.tableFileName(fileNumber);
            File tableFile = new File(databaseDir, tableFileName);
            try (FileInputStream fis = new FileInputStream(tableFile);
                 FileChannel fileChannel = fis.getChannel()) {
                if (Iq80DBFactory.USE_MMAP) {
                    table = new MMapTable(tableFile.getAbsolutePath(), fileChannel, userComparator, verifyCheckSums);
                } else {
                    table = new FileChannelTable(tableFile.getAbsolutePath(), fileChannel, userComparator, verifyCheckSums);
                }
            }
        }

        public Table getTable() {
            return table;
        }
    }
}