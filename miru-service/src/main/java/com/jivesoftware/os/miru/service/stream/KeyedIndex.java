package com.jivesoftware.os.miru.service.stream;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.filer.io.api.KeyRange;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import java.util.Comparator;
import java.util.List;

/**
 *
 */
public interface KeyedIndex {

    Comparator<byte[]> LEX_COMPARATOR = (o1, o2) -> UnsignedBytes.lexicographicalComparator().compare(o1, o2);
    Comparator<MiruIBA> IBA_COMPARATOR = (o1, o2) -> UnsignedBytes.lexicographicalComparator().compare(o1.getBytes(), o2.getBytes());

    void close();

    void stream(byte[] from, byte[] to, KeyValueStream keyValueStream);

    void reverseStream(byte[] from, byte[] to, KeyValueStream keyValueStream);

    void streamKeys(List<KeyRange> ranges, KeyStream keyStream);

    byte[] get(byte[] keyBytes);

    boolean contains(byte[] keyBytes);

    void put(byte[] keyBytes, byte[] valueBytes);

    void copyTo(KeyedIndex keyedIndex);

    interface KeyValueStream {

        boolean stream(byte[] keyBytes, byte[] valueBytes);
    }

    interface KeyStream {

        boolean stream(byte[] keyBytes);
    }
}
