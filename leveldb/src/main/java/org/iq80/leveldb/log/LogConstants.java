package org.iq80.leveldb.log;

import static org.iq80.leveldb.util.SizeOf.*;

/**
 * @author
 */
public final class LogConstants {

    public static final int BLOCK_SIZE = 32768;

    /**
     * Header is checksum (4 bytes), type (1 byte), length (2 bytes).
     */
    public static final int HEADER_SIZE = SIZE_OF_INT + SIZE_OF_BYTE + SIZE_OF_SHORT;

}