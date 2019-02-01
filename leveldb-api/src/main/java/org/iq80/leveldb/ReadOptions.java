package org.iq80.leveldb;

/**
 * @author
 */
public class ReadOptions {
    private boolean verifyChecksums;
    private boolean fillCache = true;
    private Snapshot snapshot;

    /**
     * 设置属性
     * @param snapshot
     * @return
     */
    public ReadOptions snapshot(Snapshot snapshot) {
        this.snapshot = snapshot;
        return this;
    }

    public ReadOptions fillCache(boolean fillCache) {
        this.fillCache = fillCache;
        return this;
    }

    public ReadOptions verifyChecksums(boolean verifyChecksums) {
        this.verifyChecksums = verifyChecksums;
        return this;
    }

    /**
     * 读取属性
     */

    public boolean fillCache() {
        return fillCache;
    }

    public boolean verifyChecksums() {
        return verifyChecksums;
    }

    public Snapshot snapshot() {
        return snapshot;
    }
}