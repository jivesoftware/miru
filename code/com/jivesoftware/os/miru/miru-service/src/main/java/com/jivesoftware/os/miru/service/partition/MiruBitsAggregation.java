package com.jivesoftware.os.miru.service.partition;

import com.googlecode.javaewah.BitmapStorage;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;

public class MiruBitsAggregation {

    public static void and(final BitmapStorage container,
            final EWAHCompressedBitmap... bitmaps) {
        if (bitmaps.length < 2) {
            throw new IllegalArgumentException("We need at least two bitmaps");
        }
        PriorityQueue<EWAHCompressedBitmap> pq = new PriorityQueue<>(bitmaps.length,
                new Comparator<EWAHCompressedBitmap>() {
                    @Override
                    public int compare(EWAHCompressedBitmap a, EWAHCompressedBitmap b) {
                        return a.sizeInBytes() - b.sizeInBytes();
                    }
                });
        for (EWAHCompressedBitmap x : bitmaps) {
            pq.add(x);
        }
        while (pq.size() > 2) {
            EWAHCompressedBitmap x1 = pq.poll();
            EWAHCompressedBitmap x2 = pq.poll();
            pq.add(x1.and(x2));
        }
        pq.poll().andToContainer(pq.poll(), container);
    }

    /**
     * Unlike other supported aggregations, pButNotQ (andNot) is NOT commutative: (P & ~Q) != (Q & ~P),
     * so we cannot sort the full array. The first element in the array is P, and the rest are ~Qs.
     *
     * Note: (P & ~Q1 & ~Q2 & ~Q3) is the same as (P & ~(Q1 | Q2 | Q3)), so we can leverage the
     * {@link #or} optimization for a subsection of the array.
     *
     * @param container the container
     * @param bitmaps the bitmaps
     */
    public static void pButNotQ(final BitmapStorage container, final EWAHCompressedBitmap... bitmaps) {
        if (bitmaps.length < 2) {
            throw new IllegalArgumentException("We need at least two bitmaps");
        }

        EWAHCompressedBitmap p = bitmaps[0];
        EWAHCompressedBitmap q;
        if (bitmaps.length == 2) {
            q = bitmaps[1];
        } else {
            q = new EWAHCompressedBitmap();
            or(q, Arrays.copyOfRange(bitmaps, 1, bitmaps.length));
        }

        p.andNotToContainer(q, container);
    }

    public static void or(final BitmapStorage container,
            final EWAHCompressedBitmap... bitmaps) {
        if (bitmaps.length < 2) {
            throw new IllegalArgumentException("We need at least two bitmaps");
        }
        PriorityQueue<EWAHCompressedBitmap> pq = new PriorityQueue<>(bitmaps.length,
                new Comparator<EWAHCompressedBitmap>() {
                    @Override
                    public int compare(EWAHCompressedBitmap a, EWAHCompressedBitmap b) {
                        return a.sizeInBytes() - b.sizeInBytes();
                    }
                });
        for (EWAHCompressedBitmap x : bitmaps) {
            pq.add(x);
        }
        while (pq.size() > 2) {
            EWAHCompressedBitmap x1 = pq.poll();
            EWAHCompressedBitmap x2 = pq.poll();
            pq.add(x1.or(x2));
        }
        pq.poll().orToContainer(pq.poll(), container);
    }

    public static void xor(final BitmapStorage container,
            final EWAHCompressedBitmap... bitmaps) {
        if (bitmaps.length < 2) {
            throw new IllegalArgumentException("We need at least two bitmaps");
        }
        PriorityQueue<EWAHCompressedBitmap> pq = new PriorityQueue<>(bitmaps.length,
                new Comparator<EWAHCompressedBitmap>() {
                    @Override
                    public int compare(EWAHCompressedBitmap a, EWAHCompressedBitmap b) {
                        return a.sizeInBytes() - b.sizeInBytes();
                    }
                });
        for (EWAHCompressedBitmap x : bitmaps) {
            pq.add(x);
        }
        while (pq.size() > 2) {
            EWAHCompressedBitmap x1 = pq.poll();
            EWAHCompressedBitmap x2 = pq.poll();
            pq.add(x1.xor(x2));
        }
        pq.poll().xorToContainer(pq.poll(), container);
    }




}
