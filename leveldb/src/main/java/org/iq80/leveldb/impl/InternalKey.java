package org.iq80.leveldb.impl;

import lombok.Getter;
import org.iq80.leveldb.slice.Slice;
import org.iq80.leveldb.slice.SliceOutput;
import org.iq80.leveldb.slice.Slices;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_LONG;

/**
 * @author
 */
public class InternalKey {
    @Getter
    private final Slice userKey;
    @Getter
    private final long sequenceNumber;
    @Getter
    private final ValueType valueType;

    public InternalKey(Slice userKey, long sequenceNumber, ValueType valueType) {
        requireNonNull(userKey, "userKey is null");
        checkArgument(sequenceNumber >= 0, "sequenceNumber is negative");
        requireNonNull(valueType, "valueType is null");

        this.userKey = userKey;
        this.sequenceNumber = sequenceNumber;
        this.valueType = valueType;
    }

    public InternalKey(byte[] data) {
        this(Slices.wrappedBuffer(data));
    }

    public InternalKey(Slice data) {
        requireNonNull(data, "data is null");
        checkArgument(data.length() >= SIZE_OF_LONG, "data must be at least %s bytes", SIZE_OF_LONG);
        this.userKey = getUserKey(data);
        long packedSequenceAndType = data.getLong(data.length() - SIZE_OF_LONG);
        this.sequenceNumber = SequenceNumber.unpackSequenceNumber(packedSequenceAndType);
        this.valueType = SequenceNumber.unpackValueType(packedSequenceAndType);
    }

    public Slice encode() {
        Slice slice = Slices.allocate(userKey.length() + SIZE_OF_LONG);
        SliceOutput sliceOutput = slice.output();
        sliceOutput.writeBytes(userKey);
        sliceOutput.writeLong(SequenceNumber.packSequenceAndValueType(sequenceNumber, valueType));
        return slice;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InternalKey that = (InternalKey) o;

        if (sequenceNumber != that.sequenceNumber) {
            return false;
        }
        if (userKey != null ? !userKey.equals(that.userKey) : that.userKey != null) {
            return false;
        }
        if (valueType != that.valueType) {
            return false;
        }
        return true;
    }

    private int hash;

    @Override
    public int hashCode() {
        if (hash == 0) {
            int result = userKey != null ? userKey.hashCode() : 0;
            result = 31 * result + (int) (sequenceNumber ^ (sequenceNumber >>> 32));
            result = 31 * result + (valueType != null ? valueType.hashCode() : 0);
            if (result == 0) {
                result = 1;
            }
            hash = result;
        }
        return hash;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("InternalKey");
        // todo don't print the real value
        sb.append("{key=").append(getUserKey().toString(UTF_8));
        sb.append(", sequenceNumber=").append(getSequenceNumber());
        sb.append(", valueType=").append(getValueType());
        sb.append('}');
        return sb.toString();
    }

    private static Slice getUserKey(Slice data) {
        return data.slice(0, data.length() - SIZE_OF_LONG);
    }
}
