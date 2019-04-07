package org.iq80.leveldb.log;

public interface LogMonitor {
    void corruption(long bytes, String reason);
    void corruption(long bytes, Throwable reason);
}