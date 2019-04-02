package org.iq80.leveldb.impl;

import lombok.Data;

/**
 * @author yf
 */
@Data
public class ReadStats {
    private int seekFileLevel = -1;
    private FileMetaData seekFile;

    public void clear() {
        seekFileLevel = -1;
        seekFile = null;
    }
}