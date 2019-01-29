package org.iq80.leveldb.impl;

import lombok.Getter;
import lombok.ToString;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author xxx
 */
@ToString
public class FileMetaData
{
    /**
     * 文件编号
     */
    @Getter
    private final long number;

    /**
     * 文件大小 bytes
     */
    @Getter
    private final long fileSize;

    /**
     * Smallest internal key served by table
     */
    @Getter
    private final InternalKey smallest;

    /**
     * Largest internal key served by table
     */
    @Getter
    private final InternalKey largest;

    /**
     * Seeks allowed until compaction
     * TODO this mutable state should be moved elsewhere
     */
    private final AtomicInteger allowedSeeks = new AtomicInteger(1 << 30);

    public FileMetaData(long number, long fileSize, InternalKey smallest, InternalKey largest)
    {
        this.number = number;
        this.fileSize = fileSize;
        this.smallest = smallest;
        this.largest = largest;
    }

    public int getAllowedSeeks()
    {
        return allowedSeeks.get();
    }

    public void setAllowedSeeks(int allowedSeeks)
    {
        this.allowedSeeks.set(allowedSeeks);
    }

    public void decrementAllowedSeeks()
    {
        allowedSeeks.getAndDecrement();
    }
}