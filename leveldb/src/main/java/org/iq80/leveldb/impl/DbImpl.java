package org.iq80.leveldb.impl;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.iq80.leveldb.*;
import org.iq80.leveldb.impl.Filename.FileInfo;
import org.iq80.leveldb.impl.Filename.FileType;
import org.iq80.leveldb.impl.MemTable.MemTableIterator;
import org.iq80.leveldb.impl.WriteBatchImpl.Handler;
import org.iq80.leveldb.log.*;
import org.iq80.leveldb.slice.Slice;
import org.iq80.leveldb.slice.SliceInput;
import org.iq80.leveldb.slice.SliceOutput;
import org.iq80.leveldb.slice.Slices;
import org.iq80.leveldb.table.BytewiseComparator;
import org.iq80.leveldb.table.CustomUserComparator;
import org.iq80.leveldb.table.TableBuilder;
import org.iq80.leveldb.table.UserComparator;
import org.iq80.leveldb.util.DbIterator;
import org.iq80.leveldb.util.FileUtils;
import org.iq80.leveldb.util.MergingIterator;
import org.iq80.leveldb.util.Snappy;

import java.io.*;
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.iq80.leveldb.impl.DbConstants.*;
import static org.iq80.leveldb.impl.SequenceNumber.MAX_SEQUENCE_NUMBER;
import static org.iq80.leveldb.impl.ValueType.DELETION;
import static org.iq80.leveldb.impl.ValueType.VALUE;
import static org.iq80.leveldb.slice.Slices.readLengthPrefixedBytes;
import static org.iq80.leveldb.slice.Slices.writeLengthPrefixedBytes;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_INT;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_LONG;

/**
 * 这里通过成员变量可以得到整个DB的逻辑结构：
 * >>> 1. 两个内存表：memTable和immutableMemTable
 * >>> 2. 物理存储：VersionSet
 * >>> 3. 日志writer: LogWriter
 * DB提供两个核心能力:
 * 1. 写入K-V数据
 * >>> a. 将K-V操作写入日志文件
 * >>> b. 更新memTable
 * 2. 通过K查找V
 * >>> a. 优先查找memTable，查到直接返回
 * >>> b. 在immutableMemTable中查找，找到就返回，注意immutableMemTable可能为null,此时直接进入下一步
 * >>> c. 通过VersionSet查找K对应的V
 *
 * @author yf
 */
@Slf4j
@SuppressWarnings("AccessingNonPublicFieldOfAnotherObject")
public class DbImpl implements DB {
    private final Options options;
    private final File databaseDir;
    private final TableCache tableCache;
    private final DbLock dbLock;
    private final VersionSet versionSet;

    private final AtomicBoolean shuttingDown = new AtomicBoolean();
    private final ReentrantLock mutex = new ReentrantLock();
    private final Condition backgroundCondition = mutex.newCondition();

    private final List<Long> pendingOutputs = new ArrayList<>();

    /**
     * 写操作将通过LogWriter写入日志中，防止应用重启等导致的数据丢失
     */
    private LogWriter logWriter;

    private MemTable memTable;
    private MemTable immutableMemTable;

    private final InternalKeyComparator internalKeyComparator;

    private volatile Throwable backgroundException;
    private final ExecutorService compactionExecutor;
    private Future<?> backgroundCompaction;

    private ManualCompaction manualCompaction;

    public DbImpl(Options options, File databaseDir) throws IOException {
        //入参校验
        requireNonNull(options, "options is null");
        requireNonNull(databaseDir, "databaseDir is null");

        //修复db选项
        this.options = options;
        if (this.options.compressionType() == CompressionType.SNAPPY && !Snappy.available()) {
            this.options.compressionType(CompressionType.NONE);
        }

        this.databaseDir = databaseDir;

        //初始化key比较器
        DBComparator comparator = options.comparator();
        UserComparator userComparator;
        if (comparator != null) {
            userComparator = new CustomUserComparator(comparator);
        } else {
            userComparator = new BytewiseComparator();
        }
        internalKeyComparator = new InternalKeyComparator(userComparator);
        memTable = new MemTable(internalKeyComparator);
        immutableMemTable = null;

        // compaction操作固定线程池
        compactionExecutor = Executors.newSingleThreadExecutor(compactionThreadFactory());

        // Reserve ten files or so for other uses and give the rest to TableCache.
        int tableCacheSize = options.maxOpenFiles() - 10;
        tableCache = new TableCache(databaseDir, tableCacheSize, new InternalUserComparator(internalKeyComparator), options.verifyChecksums());

        // 目录不存在则创建
        databaseDir.mkdirs();
        checkArgument(databaseDir.exists(), "Database directory '%s' does not exist and could not be created", databaseDir);
        checkArgument(databaseDir.isDirectory(), "Database directory '%s' is not a directory", databaseDir);

        mutex.lock();
        try {
            // lock the database dir
            dbLock = new DbLock(new File(databaseDir, Filename.lockFileName()));

            // 校验文件 "current"
            File currentFile = new File(databaseDir, Filename.currentFileName());
            if (!currentFile.canRead()) {
                checkArgument(options.createIfMissing(), "Database '%s' does not exist and the create if missing option is disabled", databaseDir);
            } else {
                checkArgument(!options.errorIfExists(), "Database '%s' exists and the error if exists option is enabled", databaseDir);
            }

            versionSet = new VersionSet(databaseDir, tableCache, internalKeyComparator);

            // load  (and recover) current version
            versionSet.recover();

            // Recover from all newer logWriter files than the ones named in the
            // descriptor (new logWriter files may have been added by the previous
            // incarnation without registering them in the descriptor).
            //
            // Note that PrevLogNumber() is no longer used, but we pay
            // attention to it in case we are recovering a database
            // produced by an older version of leveldb.
            List<Long> logFileNumbers = FileUtils.getLogFileNums(databaseDir, versionSet.getLogNumber(), versionSet.getPrevLogNumber());

            // Recover in the order in which the logs were generated
            VersionEdit edit = new VersionEdit();
            Collections.sort(logFileNumbers);
            for (Long fileNumber : logFileNumbers) {
                /**
                 * 基于logFile做恢复,将txLog还原为memTable结构，然后将memTable直接dump到level0
                 */
                long maxSequence = recoverLogFile(fileNumber, edit);
                if (versionSet.getLastSequence() < maxSequence) {
                    versionSet.setLastSequence(maxSequence);
                }
            }

            // open transaction logWriter
            long logFileNumber = versionSet.getNextFileNumber();
            File txLogFile = new File(databaseDir, Filename.logFileName(logFileNumber));
            this.logWriter = Logs.createLogWriter(txLogFile, logFileNumber);

            edit.setLogNumber(logWriter.getFileNumber());
            log.info("将事务日志文件从切换至:{}", txLogFile.getName());
            // apply recovered edits
            versionSet.logAndApply(edit);

            // cleanup unused files
            deleteObsoleteFiles();

            //启动后代compaction任务
            maybeScheduleCompaction();
        } finally {
            mutex.unlock();
        }
    }

    private ThreadFactory compactionThreadFactory() {
        return new ThreadFactoryBuilder()
                .setNameFormat("leveldb-compaction-%s")
                .setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        System.out.printf("%s%n", t);
                    }
                })
                .build();
    }

    @Override
    public void close() {
        log.info("DB执行close方法，准备退出.....");
        if (shuttingDown.getAndSet(true)) {
            return;
        }

        mutex.lock();
        try {
            while (backgroundCompaction != null) {
                backgroundCondition.awaitUninterruptibly();
            }
        } finally {
            mutex.unlock();
        }

        compactionExecutor.shutdown();
        try {
            compactionExecutor.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            log.info("VersionSet执行destroy方法");
            versionSet.destroy();
        } catch (IOException ignored) {
        }
        try {
            logWriter.close();
        } catch (IOException ignored) {
        }
        tableCache.close();
        dbLock.release();
    }

    @Override
    public String getProperty(String name) {
        checkBackgroundException();
        return null;
    }

    /**
     * 清理非活跃文件,这里通过文件版本号进行判断,范围涉及四种的文件
     * >>>> log后缀
     * >>>> MANIFEST
     * >>>> sst后缀
     * >>>> tmp后缀
     */
    private void deleteObsoleteFiles() {
        log.info("执行deleteObsoleteFiles()方法.....");
        checkState(mutex.isHeldByCurrentThread());

        // Make a set of all of the live files
        List<Long> live = new ArrayList<>(this.pendingOutputs);
        for (FileMetaData fileMetaData : versionSet.getLiveFiles()) {
            live.add(fileMetaData.getNumber());
        }

        for (File file : Filename.listFiles(databaseDir)) {
            FileInfo fileInfo = Filename.parseFileName(file);
            if (fileInfo == null) {
                continue;
            }
            long number = fileInfo.getFileNumber();
            boolean keep = true;
            switch (fileInfo.getFileType()) {
                case LOG:
                    keep = ((number >= versionSet.getLogNumber()) || (number == versionSet.getPrevLogNumber()));
                    break;
                case DESCRIPTOR:
                    // Keep my manifest file, and any newer incarnations'
                    // (in case there is a race that allows other incarnations)
                    keep = (number >= versionSet.getManifestFileNumber());
                    break;
                case TABLE:
                    keep = live.contains(number);
                    break;
                case TEMP:
                    // Any temp files that are currently being written to must
                    // be recorded in pending_outputs_, which is inserted into "live"
                    keep = live.contains(number);
                    break;
                case CURRENT:
                case DB_LOCK:
                case INFO_LOG:
                    keep = true;
                    break;
            }

            if (!keep) {
                if (fileInfo.getFileType() == FileType.TABLE) {
                    log.info("缓存中淘汰文件编号:{}", number);
                    tableCache.evict(number);
                }
                log.info("删除文件:{}", file.getName());
                file.delete();
            }
        }
    }

    public void flushMemTable() {
        mutex.lock();
        try {
            // force compaction
            makeRoomForWrite(true);

            while (immutableMemTable != null) {
                backgroundCondition.awaitUninterruptibly();
            }
        } finally {
            mutex.unlock();
        }
    }

    public void compactRange(int level, Slice start, Slice end) {
        checkArgument(level >= 0, "level is negative");
        checkArgument(level + 1 < NUM_LEVELS, "level is greater than or equal to %s", NUM_LEVELS);
        requireNonNull(start, "start is null");
        requireNonNull(end, "end is null");

        mutex.lock();
        try {
            while (this.manualCompaction != null) {
                backgroundCondition.awaitUninterruptibly();
            }
            ManualCompaction manualCompaction = new ManualCompaction(level, start, end);
            this.manualCompaction = manualCompaction;

            maybeScheduleCompaction();

            while (this.manualCompaction == manualCompaction) {
                backgroundCondition.awaitUninterruptibly();
            }
        } finally {
            mutex.unlock();
        }
    }

    /**
     * 启动后台定时compaction任务
     * 1. 检查锁的持有状况，防止两个线城都启动compaction操作
     * 2. 当前已经存在一个compaction任务执行中，则直接退出
     * 3. DB处于“正在关闭中...”则直接跳出
     * 4. immutableMemTable为空，且VersionSet判断不需要compaction操作，则不做compaction
     * 以上条件都不满足，启动一个异步线城执行compaction操作
     */
    private void maybeScheduleCompaction() {
        checkState(mutex.isHeldByCurrentThread());
        log.info("DB实例的maybeScheduleCompaction()方法执行");
        if (backgroundCompaction != null) {
            log.info("后台合并正在进行中，本次compaction直接退出");
        } else if (shuttingDown.get()) {
            log.info("DB正在关闭中，放弃本次compaction");
        } else if (immutableMemTable == null && manualCompaction == null && !versionSet.needsCompaction()) {
            log.info("immutableMemTable==null且versionSet反馈不需要compaction，放弃本次合并");
        } else {
            log.info("启动异步compaction任务...");
            backgroundCompaction = compactionExecutor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    try {
                        backgroundCall();
                    } catch (DatabaseShutdownException ignored) {
                    } catch (Throwable e) {
                        backgroundException = e;
                    }
                    return null;
                }
            });
        }
    }

    public void checkBackgroundException() {
        Throwable e = backgroundException;
        if (e != null) {
            throw new BackgroundProcessingException(e);
        }
    }

    private void backgroundCall() throws IOException {
        log.info("后台线程启动compaction 开始....");
        mutex.lock();
        try {
            if (backgroundCompaction == null) {
                log.info("backgroundCompaction == null直接退出");
                return;
            }

            try {
                if (!shuttingDown.get()) {
                    log.info("系统没有进入“关闭中”，可以执行后台compaction");
                    backgroundCompaction();
                }
            } finally {
                backgroundCompaction = null;
            }
        } finally {
            try {
                // Previous compaction may have produced too many files in a level,
                // so reschedule another compaction if needed.
                maybeScheduleCompaction();
            } finally {
                try {
                    backgroundCondition.signalAll();
                } finally {
                    mutex.unlock();
                }
            }
        }
    }

    /**
     * 满足compaction条件后触发的实际操作逻辑
     * 1. 将immutableMemTable内容直接dump到level0文件中
     * 2.
     *
     * @throws IOException
     */
    private void backgroundCompaction() throws IOException {
        checkState(mutex.isHeldByCurrentThread());

        compactMemTableInternal();

        Compaction compaction;
        if (manualCompaction != null) {
            compaction = versionSet.compactRange(manualCompaction.level,
                    new InternalKey(manualCompaction.begin, MAX_SEQUENCE_NUMBER, VALUE),
                    new InternalKey(manualCompaction.end, 0, DELETION));
        } else {
            compaction = versionSet.pickCompaction();
        }

        if (compaction == null) {
            // no compaction
        } else if (manualCompaction == null && compaction.isTrivialMove()) {
            // Move file to next level
            checkState(compaction.getLevelInputs().size() == 1);
            FileMetaData fileMetaData = compaction.getLevelInputs().get(0);
            compaction.getEdit().deleteFile(compaction.getLevel(), fileMetaData.getNumber());
            compaction.getEdit().addFile(compaction.getLevel() + 1, fileMetaData);
            versionSet.logAndApply(compaction.getEdit());
            // logWriter
        } else {
            CompactionState compactionState = new CompactionState(compaction);
            doCompactionWork(compactionState);
            cleanupCompaction(compactionState);
        }

        // manual compaction complete
        if (manualCompaction != null) {
            manualCompaction = null;
        }
    }

    private void cleanupCompaction(CompactionState compactionState) {
        checkState(mutex.isHeldByCurrentThread());

        if (compactionState.builder != null) {
            compactionState.builder.abandon();
        } else {
            checkArgument(compactionState.outfile == null);
        }
        for (FileMetaData output : compactionState.outputs) {
            pendingOutputs.remove(output.getNumber());
        }
    }

    /**
     * 整个DB初始化时触发，根据指定编号的日志文件做数据恢复为memTable内存结构，然后将memTable直接dump到level0中
     *
     * @param fileNumber 日志文件编号
     * @param edit
     * @return
     * @throws IOException
     */
    private long recoverLogFile(long fileNumber, VersionEdit edit) throws IOException {
        checkState(mutex.isHeldByCurrentThread());

        File file = new File(databaseDir, Filename.logFileName(fileNumber));
        log.info("基于日志文件{}做数据恢复>>>>>>>>>>>>开始", file.getName());

        try (FileInputStream fis = new FileInputStream(file); FileChannel channel = fis.getChannel()) {
            LogMonitor logMonitor = LogMonitors.logMonitor();
            LogReader logReader = new LogReader(channel, logMonitor, true, 0);

            // Read all the records and add to a memtable
            long maxSequence = 0;
            MemTable memTable = null;
            for (Slice record = logReader.readRecord(); record != null; record = logReader.readRecord()) {
                SliceInput sliceInput = record.input();
                // read header
                if (sliceInput.available() < 12) {
                    logMonitor.corruption(sliceInput.available(), "logWriter record too small");
                    continue;
                }
                long sequenceBegin = sliceInput.readLong();
                int updateSize = sliceInput.readInt();

                // read entries
                WriteBatchImpl writeBatch = WriteBatchUtils.readWriteBatch(sliceInput, updateSize);

                // apply entries to memTable
                if (memTable == null) {
                    memTable = new MemTable(internalKeyComparator);
                }
                writeBatch.forEach(new InsertIntoHandler(memTable, sequenceBegin));

                // update the maxSequence
                long lastSequence = sequenceBegin + updateSize - 1;
                if (lastSequence > maxSequence) {
                    maxSequence = lastSequence;
                }

                // flush mem table if necessary
                if (memTable.approximateMemoryUsage() > options.writeBufferSize()) {
                    writeLevel0Table(memTable, edit, null);
                    memTable = null;
                }
            }

            // flush mem table
            if (memTable != null && !memTable.isEmpty()) {
                writeLevel0Table(memTable, edit, null);
            }

            log.info("基于日志文件{}做数据恢复>>>>>>>>>>>>完成", file.getName());
            return maxSequence;
        }

    }

    @Override
    public byte[] get(byte[] key) throws DBException {
        return get(key, new ReadOptions());
    }

    @Override
    public byte[] get(byte[] key, ReadOptions options) throws DBException {
        checkBackgroundException();
        LookupKey lookupKey;
        mutex.lock();
        try {
            SnapshotImpl snapshot = getSnapshot(options);
            lookupKey = new LookupKey(Slices.wrappedBuffer(key), snapshot.getLastSequence());

            // step 1 : 先从 memTable 中查找
            LookupResult lookupResult = memTable.get(lookupKey);
            if (lookupResult != null) {
                return getRresult(lookupResult);
            }

            // step 2 : 从 immutableMemTable 中查找
            if (immutableMemTable != null) {
                log.info("进入immutableMemTable查找");
                lookupResult = immutableMemTable.get(lookupKey);
                if (lookupResult != null) {
                    return getRresult(lookupResult);
                }
            }
        } finally {
            mutex.unlock();
        }

        // step 3 : 从SST文件中查找
        LookupResult lookupResult = versionSet.get(lookupKey);

        // 检查看是否需要做后台merge操作
        mutex.lock();
        try {
            if (versionSet.needsCompaction()) {
                maybeScheduleCompaction();
            }
        } finally {
            mutex.unlock();
        }

        //查询结果
        if (lookupResult == null) {
            return null;
        }
        return getRresult(lookupResult);
    }

    private byte[] getRresult(LookupResult lookupResult) {
        Slice value = lookupResult.getValue();
        if (value != null) {
            return value.getBytes();
        }
        return null;
    }

    @Override
    public void put(byte[] key, byte[] value) throws DBException {
        put(key, value, new WriteOptions());
    }

    @Override
    public Snapshot put(byte[] key, byte[] value, WriteOptions options) throws DBException {
        return writeInternal(new WriteBatchImpl().put(key, value), options);
    }

    @Override
    public void delete(byte[] key) throws DBException {
        writeInternal(new WriteBatchImpl().delete(key), new WriteOptions());
    }

    @Override
    public Snapshot delete(byte[] key, WriteOptions options) throws DBException {
        return writeInternal(new WriteBatchImpl().delete(key), options);
    }

    @Override
    public void write(WriteBatch updates) throws DBException {
        writeInternal((WriteBatchImpl) updates, new WriteOptions());
    }

    @Override
    public Snapshot write(WriteBatch updates, WriteOptions options) throws DBException {
        return writeInternal((WriteBatchImpl) updates, options);
    }

    public Snapshot writeInternal(WriteBatchImpl updates, WriteOptions options) throws DBException {
        checkBackgroundException();
        mutex.lock();
        try {
            long sequenceEnd;
            if (updates.size() != 0) {
                //step 1 : 为写入预留空间
                makeRoomForWrite(false);

                //step 2 : 计算新的sequence
                long sequenceBegin = versionSet.getLastSequence() + 1;
                sequenceEnd = sequenceBegin + updates.size() - 1;
                // Reserve this sequence in the version set
                versionSet.setLastSequence(sequenceEnd);

                //step 3 : 写入Log文件
                Slice record = WriteBatchUtils.writeWriteBatch(updates, sequenceBegin);
                try {
                    logWriter.addRecord(record, options.sync());
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }

                //step 4 : 更新 memtable
                updates.forEach(new InsertIntoHandler(memTable, sequenceBegin));
            } else {
                sequenceEnd = versionSet.getLastSequence();
            }

            if (options.snapshot()) {
                return new SnapshotImpl(versionSet.getCurrent(), sequenceEnd);
            } else {
                return null;
            }
        } finally {
            mutex.unlock();
        }
    }

    @Override
    public WriteBatch createWriteBatch() {
        checkBackgroundException();
        return new WriteBatchImpl();
    }

    @Override
    public SeekingIteratorAdapter iterator() {
        return iterator(new ReadOptions());
    }

    @Override
    public SeekingIteratorAdapter iterator(ReadOptions options) {
        checkBackgroundException();
        mutex.lock();
        try {
            DbIterator rawIterator = internalIterator();

            // filter any entries not visible in our snapshot
            SnapshotImpl snapshot = getSnapshot(options);
            SnapshotSeekingIterator snapshotIterator = new SnapshotSeekingIterator(rawIterator, snapshot, internalKeyComparator.getUserComparator());
            return new SeekingIteratorAdapter(snapshotIterator);
        } finally {
            mutex.unlock();
        }
    }

    SeekingIterable<InternalKey, Slice> internalIterable() {
        return new SeekingIterable<InternalKey, Slice>() {
            @Override
            public DbIterator iterator() {
                return internalIterator();
            }
        };
    }

    DbIterator internalIterator() {
        mutex.lock();
        try {
            // merge together the memTable, immutableMemTable, and tables in version set
            MemTableIterator iterator = null;
            if (immutableMemTable != null) {
                iterator = immutableMemTable.iterator();
            }
            Version current = versionSet.getCurrent();
            return new DbIterator(memTable.iterator(), iterator, current.getLevel0Files(), current.getLevelIterators(), internalKeyComparator);
        } finally {
            mutex.unlock();
        }
    }

    @Override
    public Snapshot getSnapshot() {
        checkBackgroundException();
        mutex.lock();
        try {
            return new SnapshotImpl(versionSet.getCurrent(), versionSet.getLastSequence());
        } finally {
            mutex.unlock();
        }
    }

    private SnapshotImpl getSnapshot(ReadOptions options) {
        SnapshotImpl snapshot;
        if (options.snapshot() != null) {
            snapshot = (SnapshotImpl) options.snapshot();
        } else {
            snapshot = new SnapshotImpl(versionSet.getCurrent(), versionSet.getLastSequence());
            snapshot.close(); // To avoid holding the snapshot active..
        }
        return snapshot;
    }

    private void makeRoomForWrite(boolean force) {
        checkState(mutex.isHeldByCurrentThread());
        boolean allowDelay = !force;
        while (true) {
            if (allowDelay && versionSet.numberOfFilesInLevel(0) > L0_SLOWDOWN_WRITES_TRIGGER) {
                // We are getting close to hitting a hard limit on the number of
                // L0 files.  Rather than delaying a single write by several
                // seconds when we hit the hard limit, start delaying each
                // individual write by 1ms to reduce latency variance.  Also,
                // this delay hands over some CPU to the compaction thread in
                // case it is sharing the same core as the writer.
                try {
                    mutex.unlock();
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } finally {
                    mutex.lock();
                }

                // Do not delay a single write more than once
                allowDelay = false;
            } else if (!force && memTable.approximateMemoryUsage() <= options.writeBufferSize()) {
                //当前memTable容量还有空间，不需要开辟新memTable, 直接退出
                break;
            } else if (immutableMemTable != null) {
                // We have filled up the current memTable, but the previous
                // one is still being compacted, so we wait.
                backgroundCondition.awaitUninterruptibly();
            } else if (versionSet.numberOfFilesInLevel(0) >= L0_STOP_WRITES_TRIGGER) {
                // There are too many level-0 files.
//                Log(options_.info_log, "waiting...\n");
                backgroundCondition.awaitUninterruptibly();
            } else {
                // Attempt to switch to a new memtable and trigger compaction of old
                checkState(versionSet.getPrevLogNumber() == 0);

                // close the existing logWriter
                try {
                    log.info("关闭当前日志文件:{}", logWriter.getFile().getName());
                    logWriter.close();
                } catch (IOException e) {
                    throw new RuntimeException("Unable to close logWriter file " + logWriter.getFile(), e);
                }

                // open a new logWriter
                long logNumber = versionSet.getNextFileNumber();
                try {
                    File targetFile = new File(databaseDir, Filename.logFileName(logNumber));
                    log.info("创建新日志文件:{}", targetFile.getName());
                    this.logWriter = Logs.createLogWriter(targetFile, logNumber);
                } catch (IOException e) {
                    String errMsg = "Unable to open new logWriter file " + new File(databaseDir, Filename.logFileName(logNumber)).getAbsoluteFile();
                    throw new RuntimeException(errMsg, e);
                }

                //将immutableMemTable指向memTable进入不可写状态，并开辟一个新的memTable
                immutableMemTable = memTable;
                memTable = new MemTable(internalKeyComparator);

                // Do not force another compaction there is space available
                force = false;

                maybeScheduleCompaction();
            }
        }
    }

   /* public void compactMemTable() throws IOException {
        mutex.lock();
        try {
            compactMemTableInternal();
        } finally {
            mutex.unlock();
        }
    }*/

    /**
     * 此方法执行两个操作：
     * 1. 将immutableMemTable直接dump为level0的若干文件，释放immutableMemTable引用对象
     * 2. 版本更新
     * 3. 调用deleteObsoleteFiles方法，清理游离文件
     *
     * @throws IOException
     */
    private void compactMemTableInternal() throws IOException {
        log.info("compactMemTableInternal()方法执行....");
        checkState(mutex.isHeldByCurrentThread());
        if (immutableMemTable == null) {
            log.info("immutableMemTable == null 直接退出....");
            return;
        }

        try {
            // Save the contents of the memTable as a new Table
            VersionEdit edit = new VersionEdit();
            Version base = versionSet.getCurrent();

            // 将immutableMemTable数据写入level0中
            writeLevel0Table(immutableMemTable, edit, base);

            if (shuttingDown.get()) {
                throw new DatabaseShutdownException("Database shutdown during memtable compaction");
            }

            // Replace immutable memtable with the generated Table
            edit.setPreviousLogNumber(0L);
            edit.setLogNumber(logWriter.getFileNumber());  // Earlier logs no longer needed
            versionSet.logAndApply(edit);

            //释放immutableMemTable引用
            immutableMemTable = null;

            //清理无用文件
            deleteObsoleteFiles();
        } finally {
            backgroundCondition.signalAll();
        }
    }

    /**
     * 将memTable数据写入level0
     *
     * @param mem  可能是基于log回放构建的memTable也可能是当前DB实例的immutableMemTable成员
     * @param edit
     * @param base 当db初始化时，此参数为空
     * @throws IOException
     */
    private void writeLevel0Table(MemTable mem, VersionEdit edit, Version base) throws IOException {
        checkState(mutex.isHeldByCurrentThread());

        // skip empty mem table
        if (mem.isEmpty()) {
            log.info("将memTable数据dump到Level0文件, 由于memTable为空，直接退出");
            return;
        }

        //生成新的sst文件编号
        long fileNumber = versionSet.getNextFileNumber();
        //将新的sst文件加入到管理文件列表中
        pendingOutputs.add(fileNumber);

        mutex.unlock();
        FileMetaData fileMeta;
        try {
            fileMeta = dumpMemTableToSST(mem, fileNumber);
        } finally {
            mutex.lock();
        }

        //将新的sst文件从管理文件列表中移除
        pendingOutputs.remove(fileNumber);

        // Note that if file size is zero, the file has been deleted and
        // should not be added to the manifest.
        int level = 0;
        if (fileMeta != null && fileMeta.getFileSize() > 0) {
            Slice minUserKey = fileMeta.getSmallest().getUserKey();
            Slice maxUserKey = fileMeta.getLargest().getUserKey();
            if (base != null) {
                level = base.pickLevelForMemTableOutput(minUserKey, maxUserKey);
            }
            edit.addFile(level, fileMeta);
        }
    }

    /**
     * 将memTable的数据dump到sst文件中
     *
     * @param data       数据集合
     * @param fileNumber sst文件编号
     * @return sst表格文件的元数据
     * @throws IOException
     */
    private FileMetaData dumpMemTableToSST(SeekingIterable<InternalKey, Slice> data, long fileNumber) throws IOException {
        File file = new File(databaseDir, Filename.tableFileName(fileNumber));
        log.info("dump memTable内容到{}中", file.getName());
        try {
            InternalKey smallest = null;
            InternalKey largest = null;
            FileChannel channel = new FileOutputStream(file).getChannel();
            try {
                //构建一个 tableBuilder
                TableBuilder tableBuilder = new TableBuilder(options, channel, new InternalUserComparator(internalKeyComparator));

                //遍历memTable的键值对
                for (Entry<InternalKey, Slice> entry : data) {
                    // update keys
                    InternalKey key = entry.getKey();
                    log.info("dump memTable >>>>>>> 处理键: {}", key);
                    if (smallest == null) {
                        smallest = key;
                    }
                    largest = key;

                    tableBuilder.add(key.encode(), entry.getValue());
                }
                tableBuilder.finish();
            } finally {

                //强制刷新
                try {
                    channel.force(true);
                } finally {
                    channel.close();
                }
            }

            if (smallest == null) {
                return null;
            }
            FileMetaData fileMetaData = new FileMetaData(fileNumber, file.length(), smallest, largest);

            // verify table can be opened
            tableCache.newIterator(fileMetaData);

            pendingOutputs.remove(fileNumber);
            return fileMetaData;
        } catch (IOException e) {
            file.delete();
            throw e;
        }
    }

    private void doCompactionWork(CompactionState compactionState) throws IOException {
        checkState(mutex.isHeldByCurrentThread());
        checkArgument(versionSet.numberOfBytesInLevel(compactionState.getCompaction().getLevel()) > 0);
        checkArgument(compactionState.builder == null);
        checkArgument(compactionState.outfile == null);

        compactionState.smallestSnapshot = versionSet.getLastSequence();

        // Release mutex while we're actually doing the compaction work
        mutex.unlock();
        try {
            MergingIterator iterator = versionSet.makeInputIterator(compactionState.compaction);

            Slice currentUserKey = null;
            boolean hasCurrentUserKey = false;

            long lastSequenceForKey = MAX_SEQUENCE_NUMBER;
            while (iterator.hasNext() && !shuttingDown.get()) {
                // always give priority to compacting the current mem table
                mutex.lock();
                try {
                    compactMemTableInternal();
                } finally {
                    mutex.unlock();
                }

                InternalKey key = iterator.peek().getKey();
                if (compactionState.compaction.shouldStopBefore(key) && compactionState.builder != null) {
                    finishCompactionOutputFile(compactionState);
                }

                // Handle key/value, add to state, etc.
                boolean drop = false;
                // todo if key doesn't parse (it is corrupted),
                if (false /*!ParseInternalKey(key, &ikey)*/) {
                    // do not hide error keys
                    currentUserKey = null;
                    hasCurrentUserKey = false;
                    lastSequenceForKey = MAX_SEQUENCE_NUMBER;
                } else {
                    if (!hasCurrentUserKey || internalKeyComparator.getUserComparator().compare(key.getUserKey(), currentUserKey) != 0) {
                        // First occurrence of this user key
                        currentUserKey = key.getUserKey();
                        hasCurrentUserKey = true;
                        lastSequenceForKey = MAX_SEQUENCE_NUMBER;
                    }

                    if (lastSequenceForKey <= compactionState.smallestSnapshot) {
                        // Hidden by an newer entry for same user key
                        drop = true; // (A)
                    } else if (key.getValueType() == DELETION &&
                            key.getSequenceNumber() <= compactionState.smallestSnapshot &&
                            compactionState.compaction.isBaseLevelForKey(key.getUserKey())) {
                        // For this user key:
                        // (1) there is no data in higher levels
                        // (2) data in lower levels will have larger sequence numbers
                        // (3) data in layers that are being compacted here and have
                        //     smaller sequence numbers will be dropped in the next
                        //     few iterations of this loop (by rule (A) above).
                        // Therefore this deletion marker is obsolete and can be dropped.
                        drop = true;
                    }

                    lastSequenceForKey = key.getSequenceNumber();
                }

                if (!drop) {
                    // Open output file if necessary
                    if (compactionState.builder == null) {
                        openCompactionOutputFile(compactionState);
                    }
                    if (compactionState.builder.getEntryCount() == 0) {
                        compactionState.currentSmallest = key;
                    }
                    compactionState.currentLargest = key;
                    compactionState.builder.add(key.encode(), iterator.peek().getValue());

                    // Close output file if it is big enough
                    if (compactionState.builder.getFileSize() >=
                            compactionState.compaction.getMaxOutputFileSize()) {
                        finishCompactionOutputFile(compactionState);
                    }
                }
                iterator.next();
            }

            if (shuttingDown.get()) {
                throw new DatabaseShutdownException("DB shutdown during compaction");
            }
            if (compactionState.builder != null) {
                finishCompactionOutputFile(compactionState);
            }
        } finally {
            mutex.lock();
        }

        // todo port CompactionStats code
        installCompactionResults(compactionState);
    }

    private void openCompactionOutputFile(CompactionState compactionState) throws FileNotFoundException {
        requireNonNull(compactionState, "compactionState is null");
        checkArgument(compactionState.builder == null, "compactionState builder is not null");

        mutex.lock();
        try {
            long fileNumber = versionSet.getNextFileNumber();
            pendingOutputs.add(fileNumber);
            compactionState.currentFileNumber = fileNumber;
            compactionState.currentFileSize = 0;
            compactionState.currentSmallest = null;
            compactionState.currentLargest = null;

            File file = new File(databaseDir, Filename.tableFileName(fileNumber));
            compactionState.outfile = new FileOutputStream(file).getChannel();
            compactionState.builder = new TableBuilder(options, compactionState.outfile, new InternalUserComparator(internalKeyComparator));
        } finally {
            mutex.unlock();
        }
    }

    private void finishCompactionOutputFile(CompactionState compactionState) throws IOException {
        requireNonNull(compactionState, "compactionState is null");
        checkArgument(compactionState.outfile != null);
        checkArgument(compactionState.builder != null);

        long outputNumber = compactionState.currentFileNumber;
        checkArgument(outputNumber != 0);

        long currentEntries = compactionState.builder.getEntryCount();
        compactionState.builder.finish();

        long currentBytes = compactionState.builder.getFileSize();
        compactionState.currentFileSize = currentBytes;
        compactionState.totalBytes += currentBytes;

        FileMetaData currentFileMetaData = new FileMetaData(compactionState.currentFileNumber,
                compactionState.currentFileSize,
                compactionState.currentSmallest,
                compactionState.currentLargest);
        compactionState.outputs.add(currentFileMetaData);

        compactionState.builder = null;

        compactionState.outfile.force(true);
        compactionState.outfile.close();
        compactionState.outfile = null;

        if (currentEntries > 0) {
            // Verify that the table is usable
            tableCache.newIterator(outputNumber);
        }
    }

    private void installCompactionResults(CompactionState compact) throws IOException {
        checkState(mutex.isHeldByCurrentThread());

        // Add compaction outputs
        compact.compaction.addInputDeletions(compact.compaction.getEdit());
        int level = compact.compaction.getLevel();
        for (FileMetaData output : compact.outputs) {
            compact.compaction.getEdit().addFile(level + 1, output);
            pendingOutputs.remove(output.getNumber());
        }

        try {
            versionSet.logAndApply(compact.compaction.getEdit());
            deleteObsoleteFiles();
        } catch (IOException e) {
            // Compaction failed for some reason.  Simply discard the work and try again later.
            // Discard any files we may have created during this failed compaction
            for (FileMetaData output : compact.outputs) {
                File file = new File(databaseDir, Filename.tableFileName(output.getNumber()));
                file.delete();
            }
            compact.outputs.clear();
        }
    }

    int numberOfFilesInLevel(int level) {
        return versionSet.getCurrent().numberOfFilesInLevel(level);
    }

    @Override
    public long[] getApproximateSizes(Range... ranges) {
        requireNonNull(ranges, "ranges is null");
        long[] sizes = new long[ranges.length];
        for (int i = 0; i < ranges.length; i++) {
            Range range = ranges[i];
            sizes[i] = getApproximateSizes(range);
        }
        return sizes;
    }

    public long getApproximateSizes(Range range) {
        Version v = versionSet.getCurrent();

        InternalKey startKey = new InternalKey(Slices.wrappedBuffer(range.start()), MAX_SEQUENCE_NUMBER, VALUE);
        InternalKey limitKey = new InternalKey(Slices.wrappedBuffer(range.limit()), MAX_SEQUENCE_NUMBER, VALUE);
        long startOffset = v.getApproximateOffsetOf(startKey);
        long limitOffset = v.getApproximateOffsetOf(limitKey);

        return (limitOffset >= startOffset ? limitOffset - startOffset : 0);
    }

    public long getMaxNextLevelOverlappingBytes() {
        return versionSet.getMaxNextLevelOverlappingBytes();
    }

    private static class CompactionState {
        private final Compaction compaction;

        private final List<FileMetaData> outputs = new ArrayList<>();

        private long smallestSnapshot;

        // State kept for output being generated
        private FileChannel outfile;
        private TableBuilder builder;

        // Current file being generated
        private long currentFileNumber;
        private long currentFileSize;
        private InternalKey currentSmallest;
        private InternalKey currentLargest;

        private long totalBytes;

        private CompactionState(Compaction compaction) {
            this.compaction = compaction;
        }

        public Compaction getCompaction() {
            return compaction;
        }
    }

    private static class ManualCompaction {
        private final int level;
        private final Slice begin;
        private final Slice end;

        private ManualCompaction(int level, Slice begin, Slice end) {
            this.level = level;
            this.begin = begin;
            this.end = end;
        }
    }


    private static class InsertIntoHandler implements Handler {
        private long sequence;
        private final MemTable memTable;

        public InsertIntoHandler(MemTable memTable, long sequenceBegin) {
            this.memTable = memTable;
            this.sequence = sequenceBegin;
        }

        @Override
        public void put(Slice key, Slice value) {
            memTable.add(sequence++, VALUE, key, value);
        }

        @Override
        public void delete(Slice key) {
            memTable.add(sequence++, DELETION, key, Slices.EMPTY_SLICE);
        }
    }

    public static class DatabaseShutdownException extends DBException {
        private static final long serialVersionUID = -4460506240671018890L;

        public DatabaseShutdownException(String message) {
            super(message);
        }
    }

    public static class BackgroundProcessingException extends DBException {
        private static final long serialVersionUID = -8700381038929852408L;

        public BackgroundProcessingException(Throwable cause) {
            super(cause);
        }
    }

    private final Object suspensionMutex = new Object();
    private int suspensionCounter;

    @Override
    public void suspendCompactions() throws InterruptedException {
        compactionExecutor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    synchronized (suspensionMutex) {
                        suspensionCounter++;
                        suspensionMutex.notifyAll();
                        while (suspensionCounter > 0 && !compactionExecutor.isShutdown()) {
                            suspensionMutex.wait(500);
                        }
                    }
                } catch (InterruptedException e) {
                }
            }
        });
        synchronized (suspensionMutex) {
            while (suspensionCounter < 1) {
                suspensionMutex.wait();
            }
        }
    }

    @Override
    public void resumeCompactions() {
        synchronized (suspensionMutex) {
            suspensionCounter--;
            suspensionMutex.notifyAll();
        }
    }

    @Override
    public void compactRange(byte[] begin, byte[] end) throws DBException {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}