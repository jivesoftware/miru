package com.jivesoftware.os.miru.service.stream.factory;

import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.miru.service.stream.filter.MatchNoMoreThanNBitmapStorage;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 *
 */
public class MatchNoMoreThanNBitmapStorageTest {

    @Test
    public void testAndNotBytes() throws Exception {
        // specific example that was failing
        EWAHCompressedBitmap r = EWAHCompressedBitmap
            .bitmapOf(7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 24, 36, 37, 38, 39, 40, 41, 44, 45, 47, 48, 49, 51, 52, 54, 55, 56, 57, 59);
        EWAHCompressedBitmap n = EWAHCompressedBitmap.bitmapOf(81);
        EWAHCompressedBitmap container = new EWAHCompressedBitmap();
        MatchNoMoreThanNBitmapStorage storage = new MatchNoMoreThanNBitmapStorage(container, r.sizeInBits());
        r.andNotToContainer(n, storage);
        assertEquals(r, container);
        assertEquals(r.sizeInBits(), container.sizeInBits());
        assertEquals(r.sizeInBytes(), container.sizeInBytes());
    }

    @Test
    public void testAddWordAllOnes() throws Exception {
        int n = 70;
        EWAHCompressedBitmap container = new EWAHCompressedBitmap();
        MatchNoMoreThanNBitmapStorage storage = new MatchNoMoreThanNBitmapStorage(container, n);
        storage.addWord(~0l);
        storage.addWord(~0l);
        storage.addWord(~0l);
        assertEquals(container.sizeInBits(), n);
        assertEquals(container.cardinality(), n);
    }

    @Test
    public void testAddWordAllZeroes() throws Exception {
        int n = 70;
        EWAHCompressedBitmap container = new EWAHCompressedBitmap();
        MatchNoMoreThanNBitmapStorage storage = new MatchNoMoreThanNBitmapStorage(container, n);
        storage.addWord(0l);
        storage.addWord(0l);
        storage.addWord(0l);
        assertEquals(container.sizeInBits(), n);
        assertEquals(container.cardinality(), 0);
    }

    @Test
    public void testAddStreamOfLiteralWords() throws Exception {
        int n = 70;
        EWAHCompressedBitmap container = new EWAHCompressedBitmap();
        MatchNoMoreThanNBitmapStorage storage = new MatchNoMoreThanNBitmapStorage(container, n);
        storage.addStreamOfLiteralWords(convert(new long[] { ~0l, ~0l, ~0l }), 0, 3);
        assertEquals(container.sizeInBits(), n);
        assertEquals(container.cardinality(), n);
    }

    @Test
    public void testAddStreamOfEmptyWordsTrueOverflow() throws Exception {
        int n = 70;
        EWAHCompressedBitmap container = new EWAHCompressedBitmap();
        MatchNoMoreThanNBitmapStorage storage = new MatchNoMoreThanNBitmapStorage(container, n);
        storage.addStreamOfEmptyWords(true, 3);
        assertEquals(container.sizeInBits(), n);
        assertEquals(container.cardinality(), n);
    }

    @Test
    public void testAddStreamOfEmptyWordsTrueUnderflow() throws Exception {
        int n = 326;
        EWAHCompressedBitmap container = new EWAHCompressedBitmap();
        MatchNoMoreThanNBitmapStorage storage = new MatchNoMoreThanNBitmapStorage(container, n);
        storage.addStreamOfEmptyWords(true, 3);
        assertEquals(container.sizeInBits(), EWAHCompressedBitmap.wordinbits * 3);
        assertEquals(container.cardinality(), EWAHCompressedBitmap.wordinbits * 3);
    }

    @Test
    public void testAddStreamOfEmptyWordsFalseOverflow() throws Exception {
        int n = 70;
        EWAHCompressedBitmap container = new EWAHCompressedBitmap();
        MatchNoMoreThanNBitmapStorage storage = new MatchNoMoreThanNBitmapStorage(container, n);
        storage.addWord(~0l);
        storage.addStreamOfEmptyWords(false, 2);
        assertEquals(container.sizeInBits(), n);
        assertEquals(container.cardinality(), EWAHCompressedBitmap.wordinbits);
    }

    @Test
    public void testAddStreamOfEmptyWordsFalseUnderflow() throws Exception {
        int n = 326;
        EWAHCompressedBitmap container = new EWAHCompressedBitmap();
        MatchNoMoreThanNBitmapStorage storage = new MatchNoMoreThanNBitmapStorage(container, n);
        storage.addWord(~0l);
        storage.addStreamOfEmptyWords(false, 2);
        assertEquals(container.sizeInBits(), EWAHCompressedBitmap.wordinbits * 3);
        assertEquals(container.cardinality(), EWAHCompressedBitmap.wordinbits);
    }

    @Test
    public void testAddStreamOfNegatedLiteralWords() throws Exception {
        int n = 70;
        EWAHCompressedBitmap container = new EWAHCompressedBitmap();
        MatchNoMoreThanNBitmapStorage storage = new MatchNoMoreThanNBitmapStorage(container, n);
        storage.addStreamOfNegatedLiteralWords(convert(new long[] { 0l, 0l, 0l }), 0, 3);
        assertEquals(container.sizeInBits(), n);
        assertEquals(container.cardinality(), n);
    }

    @Test
    public void testSetSizeInBits() throws Exception {
        int n = 70;
        EWAHCompressedBitmap container = new EWAHCompressedBitmap();
        MatchNoMoreThanNBitmapStorage storage = new MatchNoMoreThanNBitmapStorage(container, n);
        storage.addWord(~0l);
        storage.addWord(~0l);
        storage.setSizeInBits(n + 1);
        assertEquals(container.sizeInBits(), n);
        assertEquals(container.cardinality(), n);
    }

    private LongBuffer convert(long[] buffer) {
        LongBuffer longBuffer = ByteBuffer.allocateDirect(4 * 8)
            .order(ByteOrder.nativeOrder())
            .asLongBuffer();
        longBuffer.put(buffer);
        return longBuffer;
    }
}
