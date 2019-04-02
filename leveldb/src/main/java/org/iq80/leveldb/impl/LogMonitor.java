package org.iq80.leveldb.impl;

public interface LogMonitor {
    void corruption(long bytes, String reason);
    void corruption(long bytes, Throwable reason);
}