package com.jivesoftware.os.miru.service.stream;

import com.jivesoftware.os.filer.io.api.KeyRange;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DiskOrderedCursor;
import com.sleepycat.je.DiskOrderedCursorConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.util.List;

/**
 *
 */
public class BerkeleyKeyedIndex implements KeyedIndex {

    private final Database database;

    public BerkeleyKeyedIndex(Database database) {
        this.database = database;
    }

    @Override
    public void close() {
        database.close();
    }

    @Override
    public void stream(byte[] from, byte[] to, KeyValueStream keyValueStream) {
        DatabaseEntry key = new DatabaseEntry();
        if (from != null) {
            key.setData(from);
        }
        DatabaseEntry value = new DatabaseEntry();
        try (Cursor cursor = database.openCursor(null, null)) {
            if (from == null || cursor.getSearchKeyRange(key, value, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS) {
                do {
                    if (to != null && LEX_COMPARATOR.compare(key.getData(), to) >= 0) {
                        break;
                    }
                    if (!keyValueStream.stream(key.getData(), value.getData())) {
                        break;
                    }
                }
                while (cursor.getNext(key, value, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS);
            }
        }
    }

    @Override
    public void reverseStream(byte[] from, byte[] to, KeyValueStream keyValueStream) {
        DatabaseEntry key = new DatabaseEntry();
        if (from != null) {
            key.setData(from);
        }
        DatabaseEntry value = new DatabaseEntry();
        try (Cursor cursor = database.openCursor(null, null)) {
            if (from == null || cursor.getSearchKeyRange(key, value, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS) {
                if (from == null || LEX_COMPARATOR.compare(key.getData(), from) > 0) {
                    // either we're at the beginning and need to seek to end, or our 'from' does not exist and we need to rewind our cursor into range
                    if (cursor.getPrev(key, value, LockMode.READ_UNCOMMITTED) != OperationStatus.SUCCESS) {
                        return;
                    }
                }
                do {
                    if (to != null && LEX_COMPARATOR.compare(key.getData(), to) <= 0) {
                        break;
                    }
                    if (!keyValueStream.stream(key.getData(), value.getData())) {
                        break;
                    }
                }
                while (cursor.getPrev(key, value, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS);
            }
        }
    }

    @Override
    public void streamKeys(List<KeyRange> ranges, KeyStream keyStream) {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry value = new DatabaseEntry();
        value.setPartial(true);
        if (ranges != null && !ranges.isEmpty()) {
            eos:
            for (KeyRange range : ranges) {
                byte[] from = range.getStartInclusiveKey();
                byte[] to = range.getStopExclusiveKey();
                key.setData(from);
                try (Cursor cursor = database.openCursor(null, null)) {
                    if (cursor.getSearchKeyRange(key, value, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS) {
                        do {
                            if (to != null && LEX_COMPARATOR.compare(key.getData(), to) >= 0) {
                                break eos;
                            }
                            if (!keyStream.stream(key.getData())) {
                                break eos;
                            }
                        }
                        while (cursor.getNext(key, value, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS);
                    }
                }
            }
        } else {
            try (DiskOrderedCursor cursor = database.openCursor(new DiskOrderedCursorConfig().setKeysOnly(true))) {
                while (cursor.getNext(key, value, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS) {
                    if (!keyStream.stream(key.getData())) {
                        break;
                    }
                }
            }
        }
    }

    @Override
    public byte[] get(byte[] keyBytes) {
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status = database.get(null, new DatabaseEntry(keyBytes), data, LockMode.READ_UNCOMMITTED);
        return (status == OperationStatus.SUCCESS) ? data.getData() : null;
    }

    @Override
    public boolean contains(byte[] keyBytes) {
        DatabaseEntry key = new DatabaseEntry(keyBytes);
        DatabaseEntry value = new DatabaseEntry();
        value.setPartial(true);
        OperationStatus status = database.get(null, key, value, LockMode.READ_UNCOMMITTED);
        return (status == OperationStatus.SUCCESS);
    }

    @Override
    public void put(byte[] keyBytes, byte[] valueBytes) {
        database.put(null, new DatabaseEntry(keyBytes), new DatabaseEntry(valueBytes));
    }

    @Override
    public void copyTo(KeyedIndex keyedIndex) {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry value = new DatabaseEntry();
        try (DiskOrderedCursor cursor = database.openCursor(new DiskOrderedCursorConfig())) {
            while (cursor.getNext(key, value, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS) {
                keyedIndex.put(key.getData(), value.getData());
            }
        }
    }
}
