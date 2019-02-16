package org.iq80.leveldb.impl;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.iq80.leveldb.slice.DynamicSliceOutput;
import org.iq80.leveldb.slice.Slice;
import org.iq80.leveldb.slice.SliceInput;
import org.iq80.leveldb.util.VariableLengthQuantity;

import java.util.Map;
import java.util.TreeMap;

@ToString
@NoArgsConstructor
public class VersionEdit {
    @Getter
    @Setter
    private String comparatorName;
    @Getter
    @Setter
    private Long logNumber;
    @Getter
    @Setter
    private Long nextFileNumber;
    @Getter
    @Setter
    private Long previousLogNumber;
    @Getter
    @Setter
    private Long lastSequenceNumber;
    private final Map<Integer, InternalKey> compactPointers = new TreeMap<>();
    private final Multimap<Integer, FileMetaData> newFiles = ArrayListMultimap.create();
    private final Multimap<Integer, Long> deletedFiles = ArrayListMultimap.create();

    public VersionEdit(Slice slice) {
        SliceInput sliceInput = slice.input();
        while (sliceInput.isReadable()) {
            int i = VariableLengthQuantity.readVariableLengthInt(sliceInput);
            VersionEditTag tag = VersionEditTag.getValueTypeByPersistentId(i);
            tag.readValue(sliceInput, this);
        }
    }

    public Map<Integer, InternalKey> getCompactPointers() {
        return ImmutableMap.copyOf(compactPointers);
    }

    public void setCompactPointer(int level, InternalKey key) {
        compactPointers.put(level, key);
    }

    public void setCompactPointers(Map<Integer, InternalKey> compactPointers) {
        this.compactPointers.putAll(compactPointers);
    }

    public Multimap<Integer, FileMetaData> getNewFiles() {
        return ImmutableMultimap.copyOf(newFiles);
    }

    // Add the specified file at the specified level.
    // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
    // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
    public void addFile(int level, long fileNumber,
                        long fileSize,
                        InternalKey smallest,
                        InternalKey largest) {
        FileMetaData fileMetaData = new FileMetaData(fileNumber, fileSize, smallest, largest);
        addFile(level, fileMetaData);
    }

    public void addFile(int level, FileMetaData fileMetaData) {
        newFiles.put(level, fileMetaData);
    }

    public void addFiles(Multimap<Integer, FileMetaData> files) {
        newFiles.putAll(files);
    }

    public Multimap<Integer, Long> getDeletedFiles() {
        return ImmutableMultimap.copyOf(deletedFiles);
    }

    // Delete the specified "file" from the specified "level".
    public void deleteFile(int level, long fileNumber) {
        deletedFiles.put(level, fileNumber);
    }

    public Slice encode() {
        DynamicSliceOutput dynamicSliceOutput = new DynamicSliceOutput(4096);
        for (VersionEditTag versionEditTag : VersionEditTag.values()) {
            versionEditTag.writeValue(dynamicSliceOutput, this);
        }
        return dynamicSliceOutput.slice();
    }
}
