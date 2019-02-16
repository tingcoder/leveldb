package org.iq80.leveldb;

import java.io.Closeable;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public interface WriteBatch extends Closeable {
    WriteBatch put(byte[] key, byte[] value);

    WriteBatch delete(byte[] key);
}
