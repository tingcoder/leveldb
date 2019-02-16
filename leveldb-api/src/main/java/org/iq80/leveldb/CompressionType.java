package org.iq80.leveldb;

/**
 * @author
 */
public enum CompressionType {
    NONE(0x00),
    SNAPPY(0x01);
    private final int persistentId;

    CompressionType(int persistentId) {
        this.persistentId = persistentId;
    }

    public static CompressionType getCompressionTypeByPersistentId(int persistentId) {
        for (CompressionType compressionType : CompressionType.values()) {
            if (compressionType.persistentId == persistentId) {
                return compressionType;
            }
        }
        throw new IllegalArgumentException("Unknown persistentId " + persistentId);
    }

    public int persistentId() {
        return persistentId;
    }
}
