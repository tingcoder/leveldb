package org.iq80.leveldb.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.iq80.leveldb.slice.Slice;
import org.iq80.leveldb.util.InternalIterator;
import org.iq80.leveldb.util.InternalTableIterator;
import org.iq80.leveldb.util.LevelIterator;
import org.iq80.leveldb.util.MergingIterator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static com.google.common.collect.Ordering.natural;
import static java.util.Objects.requireNonNull;
import static org.iq80.leveldb.impl.DbConstants.MAX_MEM_COMPACT_LEVEL;
import static org.iq80.leveldb.impl.DbConstants.NUM_LEVELS;
import static org.iq80.leveldb.impl.SequenceNumber.MAX_SEQUENCE_NUMBER;
import static org.iq80.leveldb.impl.VersionSet.MAX_GRAND_PARENT_OVERLAP_BYTES;

/**
 * 版本对象：
 * >>> 1. 所属的VersionSet
 * >>> 2. 持有的Level0
 * >>> 3. 持有的Level集合
 * 提供数据查找服务: LookupResult get(LookupKey key)
 * >>> 先尝试从Level0查找，找到即返回
 * >>> 尝试从其他Level查找数据，找到即返回
 */
public class Version implements SeekingIterable<InternalKey, Slice> {
    private final AtomicInteger retained = new AtomicInteger(1);
    private final VersionSet versionSet;
    private final Level0 level0;
    private final List<Level> levels;

    // move these mutable fields somewhere else
    private FileMetaData fileToCompact;
    private int compactionLevel;
    private int fileToCompactLevel;
    private double compactionScore;

    public Version(VersionSet versionSet) {
        this.versionSet = versionSet;
        checkArgument(NUM_LEVELS > 1, "levels must be at least 2");

        //构建1个Level0实例
        this.level0 = new Level0(new ArrayList(), getTableCache(), getInternalKeyComparator());

        //补充6个Level实例
        Builder<Level> builder = ImmutableList.builder();
        for (int levelNum = 1; levelNum < NUM_LEVELS; levelNum++) {
            Level level = new Level(levelNum, new ArrayList(), getTableCache(), getInternalKeyComparator());
            builder.add(level);
        }
        this.levels = builder.build();
    }

    public void assertNoOverlappingFiles() {
        for (int level = 1; level < NUM_LEVELS; level++) {
            assertNoOverlappingFiles(level);
        }
    }

    public void assertNoOverlappingFiles(int level) {
        if (level <= 0) {
            return;
        }
        Collection<FileMetaData> files = getFiles().asMap().get(level);
        if (files == null) {
            return;
        }
        long previousFileNumber = 0;
        InternalKey previousEnd = null;
        for (FileMetaData fileMetaData : files) {
            if (previousEnd != null) {
                checkArgument(getInternalKeyComparator().compare(
                        previousEnd,
                        fileMetaData.getSmallest()
                ) < 0, "Overlapping files %s and %s in level %s", previousFileNumber, fileMetaData.getNumber(), level);
            }

            previousFileNumber = fileMetaData.getNumber();
            previousEnd = fileMetaData.getLargest();
        }
    }

    private TableCache getTableCache() {
        return versionSet.getTableCache();
    }

    public final InternalKeyComparator getInternalKeyComparator() {
        return versionSet.getInternalKeyComparator();
    }

    public synchronized int getCompactionLevel() {
        return compactionLevel;
    }

    public synchronized void setCompactionLevel(int compactionLevel) {
        this.compactionLevel = compactionLevel;
    }

    public synchronized double getCompactionScore() {
        return compactionScore;
    }

    public synchronized void setCompactionScore(double compactionScore) {
        this.compactionScore = compactionScore;
    }

    @Override
    public MergingIterator iterator() {
        Builder<InternalIterator> builder = ImmutableList.builder();
        builder.add(level0.iterator());
        builder.addAll(getLevelIterators());
        return new MergingIterator(builder.build(), getInternalKeyComparator());
    }

    List<InternalTableIterator> getLevel0Files() {
        Builder<InternalTableIterator> builder = ImmutableList.builder();
        for (FileMetaData file : level0.getFiles()) {
            builder.add(getTableCache().newIterator(file));
        }
        return builder.build();
    }

    List<LevelIterator> getLevelIterators() {
        Builder<LevelIterator> builder = ImmutableList.builder();
        for (Level level : levels) {
            if (!level.getFiles().isEmpty()) {
                builder.add(level.iterator());
            }
        }
        return builder.build();
    }

    public LookupResult get(LookupKey key) {
        // We can search level-by-level since entries never hop across
        // levels.  Therefore we are guaranteed that if we find data
        // in an smaller level, later levels are irrelevant.
        //保存读取命中文件的信息
        ReadStats readStats = new ReadStats();

        // step 1 : 先从Level0中查找key
        LookupResult lookupResult = level0.get(key, readStats);
        if (lookupResult == null) {
            // step 2 : 从其他所有level中查找此key
            for (Level level : levels) {
                lookupResult = level.get(key, readStats);
                if (lookupResult != null) {
                    break;
                }
            }
        }
        updateStats(readStats.getSeekFileLevel(), readStats.getSeekFile());
        return lookupResult;
    }

    int pickLevelForMemTableOutput(Slice smallestUserKey, Slice largestUserKey) {
        int level = 0;
        if (!overlapInLevel(0, smallestUserKey, largestUserKey)) {
            // Push to next level if there is no overlap in next level,
            // and the #bytes overlapping in the level after that are limited.
            InternalKey start = new InternalKey(smallestUserKey, MAX_SEQUENCE_NUMBER, ValueType.VALUE);
            InternalKey limit = new InternalKey(largestUserKey, 0, ValueType.VALUE);
            while (level < MAX_MEM_COMPACT_LEVEL) {
                if (overlapInLevel(level + 1, smallestUserKey, largestUserKey)) {
                    break;
                }
                long sum = Compaction.totalFileSize(versionSet.getOverlappingInputs(level + 2, start, limit));
                if (sum > MAX_GRAND_PARENT_OVERLAP_BYTES) {
                    break;
                }
                level++;
            }
        }
        return level;
    }

    public boolean overlapInLevel(int level, Slice smallestUserKey, Slice largestUserKey) {
        checkPositionIndex(level, levels.size(), "Invalid level");
        requireNonNull(smallestUserKey, "smallestUserKey is null");
        requireNonNull(largestUserKey, "largestUserKey is null");

        if (level == 0) {
            return level0.someFileOverlapsRange(smallestUserKey, largestUserKey);
        }
        return levels.get(level - 1).someFileOverlapsRange(smallestUserKey, largestUserKey);
    }

    public int numberOfLevels() {
        return levels.size() + 1;
    }

    public int numberOfFilesInLevel(int level) {
        if (level == 0) {
            return level0.getFiles().size();
        } else {
            return levels.get(level - 1).getFiles().size();
        }
    }

    /**
     * 返回当前Version的文件按集合，level->List<SST文件>的map结构
     *
     * @return
     */
    public Multimap<Integer, FileMetaData> getFiles() {
        ImmutableMultimap.Builder<Integer, FileMetaData> builder = ImmutableMultimap.builder();
        builder = builder.orderKeysBy(natural());

        builder.putAll(0, level0.getFiles());

        for (Level level : levels) {
            builder.putAll(level.getLevelNumber(), level.getFiles());
        }
        return builder.build();
    }



    /**
     * 根据层级拿到文件集合
     *
     * @param level level值
     * @return
     */
    public List<FileMetaData> getFiles(int level) {
        if (level == 0) {
            return level0.getFiles();
        } else {
            return levels.get(level - 1).getFiles();
        }
    }

    /**
     * 为某个level增加文件实例
     *
     * @param level        level值
     * @param fileMetaData 文件元数据
     */
    public void addFile(int level, FileMetaData fileMetaData) {
        if (level == 0) {
            level0.addFile(fileMetaData);
        } else {
            levels.get(level - 1).addFile(fileMetaData);
        }
    }

    private boolean updateStats(int seekFileLevel, FileMetaData seekFile) {
        if (seekFile == null) {
            return false;
        }

        seekFile.decrementAllowedSeeks();
        if (seekFile.getAllowedSeeks() <= 0 && fileToCompact == null) {
            fileToCompact = seekFile;
            fileToCompactLevel = seekFileLevel;
            return true;
        }
        return false;
    }

    public FileMetaData getFileToCompact() {
        return fileToCompact;
    }

    public int getFileToCompactLevel() {
        return fileToCompactLevel;
    }

    public long getApproximateOffsetOf(InternalKey key) {
        long result = 0;
        for (int level = 0; level < NUM_LEVELS; level++) {
            for (FileMetaData fileMetaData : getFiles(level)) {
                if (getInternalKeyComparator().compare(fileMetaData.getLargest(), key) <= 0) {
                    // Entire file is before "ikey", so just add the file size
                    result += fileMetaData.getFileSize();
                } else if (getInternalKeyComparator().compare(fileMetaData.getSmallest(), key) > 0) {
                    // Entire file is after "ikey", so ignore
                    if (level > 0) {
                        // Files other than level 0 are sorted by meta.smallest, so
                        // no further files in this level will contain data for
                        // "ikey".
                        break;
                    }
                } else {
                    // "ikey" falls in the range for this table.  Add the
                    // approximate offset of "ikey" within the table.
                    result += getTableCache().getApproximateOffsetOf(fileMetaData, key.encode());
                }
            }
        }
        return result;
    }

    public void retain() {
        int was = retained.getAndIncrement();
        assert was > 0 : "Version was retain after it was disposed.";
    }

    public void release() {
        int now = retained.decrementAndGet();
        assert now >= 0 : "Version was released after it was disposed.";
        if (now == 0) {
            // The version is now disposed.
            versionSet.removeVersion(this);
        }
    }

    public boolean isDisposed() {
        return retained.get() <= 0;
    }
}
