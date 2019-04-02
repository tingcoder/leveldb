package org.iq80.leveldb.impl;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.iq80.leveldb.slice.Slice;
import org.iq80.leveldb.util.Level0Iterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
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
        log.info("进入Level0查找:{} with readStats: {}", key, readStats);
        if (files.isEmpty()) {
            log.info("level0 文件集合为空，查找失败");
            return null;
        }
        List<FileMetaData> fileMetaDataList = new ArrayList<>(files.size());

        // FileMetaData记录了文件的key最小和最大值，以此判断是否需要检索哪些文件
        filterMatchFiles(key, fileMetaDataList);

        // 按照文件编号降序排序
        Collections.sort(fileMetaDataList, NEWEST_FIRST);

        readStats.clear();

        //打印日志
        List<Long> srcFileNumbers = files.stream().map(fileMetaData -> fileMetaData.getNumber()).collect(Collectors.toList());
        List<Long> targetFileNumbers = fileMetaDataList.stream().map(fileMetaData -> fileMetaData.getNumber()).collect(Collectors.toList());
        log.info("从文件编号{}过滤出目标数据文件{}进行查找", JSON.toJSONString(srcFileNumbers), JSON.toJSONString(targetFileNumbers));

        // 循环遍历关联的文件进行查找
        for (FileMetaData fileMetaData : fileMetaDataList) {
            log.info("level0 进入{}编号文件查找查找", fileMetaData.getNumber());
            LookupResult lookupResult = searchInFile(fileMetaData, key);
            if (lookupResult != null) {
                log.info("level0 进入{}编号文件查找查找成功 ..... ", fileMetaData.getNumber());
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
}