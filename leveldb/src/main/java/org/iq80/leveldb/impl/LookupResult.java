package org.iq80.leveldb.impl;

import lombok.Getter;
import org.iq80.leveldb.util.Slice;

import static java.util.Objects.requireNonNull;

/**
 * @author
 */
public class LookupResult {

    @Getter
    private final LookupKey key;
    @Getter
    private final Slice value;
    private final boolean deleted;

    public static LookupResult ok(LookupKey key, Slice value) {
        return new LookupResult(key, value, false);
    }

    public static LookupResult deleted(LookupKey key) {
        return new LookupResult(key, null, true);
    }

    private LookupResult(LookupKey key, Slice value, boolean deleted) {
        requireNonNull(key, "key is null");
        this.key = key;
        if (value != null) {
            this.value = value.slice();
        } else {
            this.value = null;
        }
        this.deleted = deleted;
    }

    public boolean isDeleted() {
        return deleted;
    }
}
