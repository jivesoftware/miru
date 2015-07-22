package com.jivesoftware.os.miru.service.stream;

import com.google.common.collect.Maps;
import com.jivesoftware.os.filer.io.api.KeyRange;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 *
 */
public class MemoryKeyedIndexStore implements KeyedIndexStore {

    private final Map<String, MemoryKeyedIndex> keyedIndexes = Maps.newConcurrentMap();

    @Override
    public void close() {
    }

    @Override
    public void delete() throws IOException {
    }

    @Override
    public void flush(boolean fsync) {
    }

    @Override
    public KeyedIndex open(String indexName) {
        return keyedIndexes.computeIfAbsent(indexName, key -> new MemoryKeyedIndex());
    }

    @Override
    public void copyTo(KeyedIndexStore indexStore) {
        for (Map.Entry<String, MemoryKeyedIndex> entry : keyedIndexes.entrySet()) {
            KeyedIndex keyedIndex = indexStore.open(entry.getKey());
            entry.getValue().copyTo(keyedIndex);
        }
    }

    private static class MemoryKeyedIndex implements KeyedIndex {

        private final ConcurrentSkipListMap<MiruIBA, byte[]> keyValues = new ConcurrentSkipListMap<>(IBA_COMPARATOR);

        @Override
        public void close() {
        }

        @Override
        public void stream(byte[] from, byte[] to, KeyValueStream keyValueStream) {
            ConcurrentNavigableMap<MiruIBA, byte[]> rangeMap;
            if (from != null && to != null) {
                rangeMap = keyValues.subMap(new MiruIBA(from), new MiruIBA(to));
            } else if (from != null) {
                rangeMap = keyValues.tailMap(new MiruIBA(from));
            } else if (to != null) {
                rangeMap = keyValues.headMap(new MiruIBA(to));
            } else {
                rangeMap = keyValues;
            }
            for (Map.Entry<MiruIBA, byte[]> entry : rangeMap.entrySet()) {
                if (!keyValueStream.stream(entry.getKey().getBytes(), entry.getValue())) {
                    break;
                }
            }
        }

        @Override
        public void reverseStream(byte[] from, byte[] to, KeyValueStream keyValueStream) {
            if (LEX_COMPARATOR.compare(from, to) > 0) {
                byte[] tmp = from;
                from = to;
                to = tmp;
            }
            ConcurrentNavigableMap<MiruIBA, byte[]> rangeMap;
            if (from != null && to != null) {
                rangeMap = keyValues.subMap(new MiruIBA(from), false, new MiruIBA(to), true);
            } else if (from != null) {
                rangeMap = keyValues.tailMap(new MiruIBA(from), false);
            } else if (to != null) {
                rangeMap = keyValues.headMap(new MiruIBA(to), true);
            } else {
                rangeMap = keyValues;
            }
            for (Map.Entry<MiruIBA, byte[]> entry : rangeMap.descendingMap().entrySet()) {
                if (!keyValueStream.stream(entry.getKey().getBytes(), entry.getValue())) {
                    break;
                }
            }
        }

        @Override
        public void streamKeys(List<KeyRange> ranges, KeyStream keyStream) {
            if (ranges != null && !ranges.isEmpty()) {
                for (KeyRange range : ranges) {
                    final Set<MiruIBA> rangeKeys = keyValues.keySet()
                        .subSet(new MiruIBA(range.getStartInclusiveKey()), new MiruIBA(range.getStopExclusiveKey()));
                    for (MiruIBA iba : rangeKeys) {
                        if (!keyStream.stream(iba.getBytes())) {
                            return;
                        }
                    }
                }
            } else {
                final Set<MiruIBA> indexKeys = keyValues.keySet();
                for (MiruIBA iba : indexKeys) {
                    if (!keyStream.stream(iba.getBytes())) {
                        return;
                    }
                }
            }
        }

        @Override
        public byte[] get(byte[] keyBytes) {
            return keyValues.get(new MiruIBA(keyBytes));
        }

        @Override
        public boolean contains(byte[] keyBytes) {
            return keyValues.containsKey(new MiruIBA(keyBytes));
        }

        @Override
        public void put(byte[] keyBytes, byte[] valueBytes) {
            keyValues.put(new MiruIBA(keyBytes), valueBytes);
        }

        @Override
        public void copyTo(KeyedIndex keyedIndex) {
            for (Map.Entry<MiruIBA, byte[]> entry : keyValues.entrySet()) {
                keyedIndex.put(entry.getKey().getBytes(), entry.getValue());
            }
        }
    }
}
