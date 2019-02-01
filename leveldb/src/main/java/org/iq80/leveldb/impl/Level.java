/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.iq80.leveldb.impl;

import com.google.common.collect.Lists;
import org.iq80.leveldb.table.UserComparator;
import org.iq80.leveldb.util.LevelIterator;
import org.iq80.leveldb.util.Slice;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static org.iq80.leveldb.impl.SequenceNumber.MAX_SEQUENCE_NUMBER;
import static org.iq80.leveldb.impl.ValueType.VALUE;

/**
 * @author TODO 此类变成不可变类型
 */
public class Level extends LevelBase implements SeekingIterable<InternalKey, Slice> {

    public Level(int levelNumber, List<FileMetaData> files, TableCache tableCache, InternalKeyComparator internalKeyComparator) {
        super(levelNumber, tableCache, internalKeyComparator, files);
        checkArgument(levelNumber >= 0, "levelNumber is negative");
    }

    @Override
    public LevelIterator iterator() {
        return createLevelConcatIterator(tableCache, files, internalKeyComparator);
    }

    public static LevelIterator createLevelConcatIterator(TableCache tableCache, List<FileMetaData> files, InternalKeyComparator internalKeyComparator) {
        return new LevelIterator(tableCache, files, internalKeyComparator);
    }

    public LookupResult get(LookupKey key, ReadStats readStats) {
        if (files.isEmpty()) {
            return null;
        }

        List<FileMetaData> fileMetaDataList = new ArrayList<>(files.size());
        if (levelNumber == 0) {
            // FileMetaData记录了文件的key最小和最大值，以此判断是否需要检索哪些文件
            filterMatchFiles(key, fileMetaDataList);
        } else {
            // Binary search to find earliest index whose largest key >= ikey.
            // 使用二分法查找 key >= ikey 的第一个文件
            List<InternalKey> largestNumberList = Lists.transform(files, FileMetaData::getLargest);
            int index = ceilingEntryIndex(largestNumberList, key.getInternalKey(), internalKeyComparator);

            // did we find any files that could contain the key?
            if (index >= files.size()) {
                return null;
            }

            // check if the smallest user key in the file is less than the target user key
            FileMetaData fileMetaData = files.get(index);
            if (internalKeyComparator.getUserComparator().compare(key.getUserKey(), fileMetaData.getSmallest().getUserKey()) < 0) {
                return null;
            }

            // search this file
            fileMetaDataList.add(fileMetaData);
        }

        FileMetaData lastFileRead = null;
        int lastFileReadLevel = -1;
        readStats.clear();
        for (FileMetaData fileMetaData : fileMetaDataList) {
            if (lastFileRead != null && readStats.getSeekFile() == null) {
                // We have had more than one seek for this read.  Charge the first file.
                readStats.setSeekFile(lastFileRead);
                readStats.setSeekFileLevel(lastFileReadLevel);
            }

            lastFileRead = fileMetaData;
            lastFileReadLevel = levelNumber;

            LookupResult lookupResult = searchInFile(fileMetaData, key);
            if (lookupResult != null) {
                return lookupResult;
            }
        }
        return null;
    }

    private static <T> int ceilingEntryIndex(List<T> list, T key, Comparator<T> comparator) {
        int insertionPoint = Collections.binarySearch(list, key, comparator);
        if (insertionPoint < 0) {
            insertionPoint = -(insertionPoint + 1);
        }
        return insertionPoint;
    }

    public boolean someFileOverlapsRange(Slice smallestUserKey, Slice largestUserKey) {
        InternalKey smallestInternalKey = new InternalKey(smallestUserKey, MAX_SEQUENCE_NUMBER, VALUE);
        int index = findFile(smallestInternalKey);

        UserComparator userComparator = internalKeyComparator.getUserComparator();
        return ((index < files.size()) && userComparator.compare(largestUserKey, files.get(index).getSmallest().getUserKey()) >= 0);
    }
}