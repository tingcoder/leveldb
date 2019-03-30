package org.iq80.leveldb.impl;

import org.iq80.leveldb.slice.Slice;
import org.iq80.leveldb.table.UserComparator;
import org.iq80.leveldb.util.Level0Iterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.iq80.leveldb.impl.SequenceNumber.MAX_SEQUENCE_NUMBER;
import static org.iq80.leveldb.impl.ValueType.VALUE;

// todo this class should be immutable
public class Level0 extends LevelBase implements SeekingIterable<InternalKey, Slice> {

    public static final Comparator<FileMetaData> NEWEST_FIRST = new Comparator<FileMetaData>() {
        @Override
        public int compare(FileMetaData fileMetaData, FileMetaData fileMetaData1) {
            return (int) (fileMetaData1.getNumber() - fileMetaData.getNumber());
        }
    };

    public Level0(List<FileMetaData> files, TableCache tableCache, InternalKeyComparator internalKeyComparator) {
        super(0, tableCache, internalKeyComparator, files);
    }

    @Override
    public Level0Iterator iterator() {
        return new Level0Iterator(tableCache, files, internalKeyComparator);
    }

    public LookupResult get(LookupKey key, ReadStats readStats) {
        if (files.isEmpty()) {
            return null;
        }
        List<FileMetaData> fileMetaDataList = new ArrayList<>(files.size());

        // FileMetaData记录了文件的key最小和最大值，以此判断是否需要检索哪些文件
        filterMatchFiles(key, fileMetaDataList);

        // 按照文件编号降序排序
        Collections.sort(fileMetaDataList, NEWEST_FIRST);

        readStats.clear();

        // 循环遍历关联的文件进行查找
        for (FileMetaData fileMetaData : fileMetaDataList) {
            LookupResult lookupResult = searchInFile(fileMetaData, key);
            if (lookupResult != null) {
                return lookupResult;
            }
            if (readStats.getSeekFile() == null) {
                // We have had more than one seek for this read.  Charge the first file.
                readStats.setSeekFile(fileMetaData);
                readStats.setSeekFileLevel(0);
            }
        }

        return null;
    }

    public boolean someFileOverlapsRange(Slice smallestUserKey, Slice largestUserKey) {
        InternalKey smallestInternalKey = new InternalKey(smallestUserKey, MAX_SEQUENCE_NUMBER, VALUE);
        int index = findFile(smallestInternalKey);

        UserComparator userComparator = internalKeyComparator.getUserComparator();
        return ((index < files.size()) && userComparator.compare(largestUserKey, files.get(index).getSmallest().getUserKey()) >= 0);
    }
}