package org.iq80.leveldb;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class Range {
    private final byte[] start;
    private final byte[] limit;

    public Range(byte[] start, byte[] limit) {
        Options.checkArgNotNull(start, "start");
        Options.checkArgNotNull(limit, "limit");
        this.limit = limit;
        this.start = start;
    }

    public byte[] limit() {
        return limit;
    }

    public byte[] start() {
        return start;
    }
}
