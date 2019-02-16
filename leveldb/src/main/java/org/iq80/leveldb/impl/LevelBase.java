package org.iq80.leveldb.impl;

import lombok.Getter;
import org.iq80.leveldb.slice.Slice;
import org.iq80.leveldb.util.InternalTableIterator;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.iq80.leveldb.impl.ValueType.VALUE;

/***
 * @author yfeng
 * @date 2019-02-01 15:50
 */
public class LevelBase {

    @Getter
    protected int levelNumber;
    /**
     * table关系缓存
     */
    protected TableCache tableCache;
    /**
     * 排序比较器
     */
    @Getter
    protected InternalKeyComparator internalKeyComparator;
    /**
     * 关联层级
     */
    @Getter
    protected List<FileMetaData> files;

    public LevelBase(int levelNumber, TableCache tableCache, InternalKeyComparator internalKeyComparator, List<FileMetaData> files) {
        requireNonNull(files, "files is null");
        requireNonNull(tableCache, "tableCache is null");
        requireNonNull(internalKeyComparator, "internalKeyComparator is null");
        this.levelNumber = levelNumber;
        this.tableCache = tableCache;
        this.internalKeyComparator = internalKeyComparator;
        this.files = files;
    }

    protected LookupResult searchInFile(FileMetaData fileMetaData, LookupKey key) {
        // open the iterator
        InternalTableIterator iterator = tableCache.newIterator(fileMetaData);

        // seek to the key
        iterator.seek(key.getInternalKey());

        if (iterator.hasNext()) {
            // parse the key in the block
            Map.Entry<InternalKey, Slice> entry = iterator.next();
            InternalKey internalKey = entry.getKey();
            checkState(internalKey != null, "Corrupt key for %s", key.getUserKey().toString(UTF_8));

            // if this is a value key (not a delete) and the keys match, return the value
            if (key.getUserKey().equals(internalKey.getUserKey())) {
                if (internalKey.getValueType() == ValueType.DELETION) {
                    return LookupResult.deleted(key);
                } else if (internalKey.getValueType() == VALUE) {
                    return LookupResult.ok(key, entry.getValue());
                }
            }
        }
        return null;
    }

    public void addFile(FileMetaData fileMetaData) {
        files.add(fileMetaData);
    }

    protected int findFile(InternalKey targetKey) {
        if (files.isEmpty()) {
            return files.size();
        }

        /**
         替换了Collections.binarySearch
         */
        int left = 0;
        int right = files.size() - 1;

        // binary search restart positions to find the restart position immediately before the targetKey
        while (left < right) {
            int mid = (left + right) / 2;

            if (internalKeyComparator.compare(files.get(mid).getLargest(), targetKey) < 0) {
                // Key at "mid.largest" is < "target".  Therefore all
                // files at or before "mid" are uninteresting.
                left = mid + 1;
            } else {
                // Key at "mid.largest" is >= "target".  Therefore all files
                // after "mid" are uninteresting.
                right = mid;
            }
        }
        return right;
    }

    protected void filterMatchFiles(LookupKey key, List<FileMetaData> fileMetaDataList) {
        for (FileMetaData fileMetaData : files) {
            if (internalKeyComparator.getUserComparator().compare(key.getUserKey(), fileMetaData.getSmallest().getUserKey()) >= 0 &&
                    internalKeyComparator.getUserComparator().compare(key.getUserKey(), fileMetaData.getLargest().getUserKey()) <= 0) {
                fileMetaDataList.add(fileMetaData);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Level");
        sb.append("{levelNumber=").append(levelNumber);
        sb.append(", files=").append(files);
        sb.append('}');
        return sb.toString();
    }
}
