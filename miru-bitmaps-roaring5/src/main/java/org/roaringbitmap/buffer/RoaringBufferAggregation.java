package org.roaringbitmap.buffer;

import java.util.Arrays;
import java.util.Collections;
import java.util.PriorityQueue;

/**
 *
 */
public class RoaringBufferAggregation {

    public static void and(final MutableRoaringBitmap answer, final MutableRoaringBitmap x1,
        final MutableRoaringBitmap x2) {

        MutableRoaringArray highLowContainer = (MutableRoaringArray) answer.highLowContainer;

        int pos1 = 0, pos2 = 0;
        final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer
            .size();
        /*
         * TODO: This could be optimized quite a bit when one bitmap is
         * much smaller than the other one.
         */
        main:
        if (pos1 < length1 && pos2 < length2) {
            short s1 = x1.highLowContainer.getKeyAtIndex(pos1);
            short s2 = x2.highLowContainer.getKeyAtIndex(pos2);
            do {
                if (s1 < s2) {
                    pos1++;
                    if (pos1 == length1) {
                        break main;
                    }
                    s1 = x1.highLowContainer.getKeyAtIndex(pos1);
                } else if (s1 > s2) {
                    pos2++;
                    if (pos2 == length2) {
                        break main;
                    }
                    s2 = x2.highLowContainer.getKeyAtIndex(pos2);
                } else {
                    final MappeableContainer c = x1.highLowContainer
                        .getContainerAtIndex(pos1)
                        .and(x2.highLowContainer.getContainerAtIndex(pos2));
                    if (c.getCardinality() > 0) {
                        highLowContainer.append(s1, c);
                    }
                    pos1++;
                    pos2++;
                    if ((pos1 == length1) || (pos2 == length2)) {
                        break main;
                    }
                    s1 = x1.highLowContainer.getKeyAtIndex(pos1);
                    s2 = x2.highLowContainer.getKeyAtIndex(pos2);
                }
            }
            while (true);
        }
    }

    public static void and(MutableRoaringBitmap container, ImmutableRoaringBitmap... bitmaps) {
        if (bitmaps.length == 1) {
            container.or(bitmaps[0]);
        } else if (bitmaps.length > 1) {
            ImmutableRoaringBitmap[] array = Arrays.copyOf(bitmaps, bitmaps.length);
            Arrays.sort(array, (a, b) -> a.getSizeInBytes() - b.getSizeInBytes());
            MutableRoaringBitmap answer = MutableRoaringBitmap.and(array[0], array[1]);
            for (int k = 2; k < array.length; ++k) {
                and(answer, answer, array[k]);
                answer.and(array[k]);
            }
            container.or(answer);
        }
    }

    public static void andNot(final MutableRoaringBitmap answer,
        final ImmutableRoaringBitmap x1,
        final ImmutableRoaringBitmap x2) {

        MutableRoaringArray highLowContainer = (MutableRoaringArray) answer.highLowContainer;

        int pos1 = 0, pos2 = 0;
        final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer.size();
        main:
        if (pos1 < length1 && pos2 < length2) {
            short s1 = x1.highLowContainer.getKeyAtIndex(pos1);
            short s2 = x2.highLowContainer.getKeyAtIndex(pos2);
            do {
                if (s1 < s2) {
                    highLowContainer.appendCopy(x1.highLowContainer.getKeyAtIndex(pos1), x1.highLowContainer.getContainerAtIndex(pos1));
                    pos1++;
                    if (pos1 == length1) {
                        break main;
                    }
                    s1 = x1.highLowContainer.getKeyAtIndex(pos1);
                } else if (s1 > s2) {
                    pos2++;
                    if (pos2 == length2) {
                        break main;
                    }
                    s2 = x2.highLowContainer.getKeyAtIndex(pos2);
                } else {
                    final MappeableContainer c = x1.highLowContainer.getContainerAtIndex(pos1).andNot(x2.highLowContainer.getContainerAtIndex(pos2));
                    if (c.getCardinality() > 0) {
                        highLowContainer.append(s1, c);
                    }
                    pos1++;
                    pos2++;
                    if ((pos1 == length1) || (pos2 == length2)) {
                        break main;
                    }
                    s1 = x1.highLowContainer.getKeyAtIndex(pos1);
                    s2 = x2.highLowContainer.getKeyAtIndex(pos2);
                }
            }
            while (true);
        }
        if (pos2 == length2) {
            highLowContainer.appendCopy(x1.highLowContainer, pos1, length1);
        }
    }

    public static void or(final MutableRoaringBitmap answer, final ImmutableRoaringBitmap x1,
        final ImmutableRoaringBitmap x2) {

        MutableRoaringArray highLowContainer = (MutableRoaringArray) answer.highLowContainer;

        int pos1 = 0, pos2 = 0;
        final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer.size();
        main:
        if (pos1 < length1 && pos2 < length2) {
            short s1 = x1.highLowContainer.getKeyAtIndex(pos1);
            short s2 = x2.highLowContainer.getKeyAtIndex(pos2);

            while (true) {
                if (s1 < s2) {
                    highLowContainer.appendCopy(x1.highLowContainer.getKeyAtIndex(pos1), x1.highLowContainer.getContainerAtIndex(pos1));
                    pos1++;
                    if (pos1 == length1) {
                        break main;
                    }
                    s1 = x1.highLowContainer.getKeyAtIndex(pos1);
                } else if (s1 > s2) {
                    highLowContainer.appendCopy(x2.highLowContainer.getKeyAtIndex(pos2), x2.highLowContainer.getContainerAtIndex(pos2));
                    pos2++;
                    if (pos2 == length2) {
                        break main;
                    }
                    s2 = x2.highLowContainer.getKeyAtIndex(pos2);
                } else {
                    highLowContainer.append(s1, x1.highLowContainer.getContainerAtIndex(pos1).or(
                            x2.highLowContainer.getContainerAtIndex(pos2))
                    );
                    pos1++;
                    pos2++;
                    if ((pos1 == length1) || (pos2 == length2)) {
                        break main;
                    }
                    s1 = x1.highLowContainer.getKeyAtIndex(pos1);
                    s2 = x2.highLowContainer.getKeyAtIndex(pos2);
                }
            }
        }
        if (pos1 == length1) {
            highLowContainer.appendCopy(x2.highLowContainer, pos2, length2);
        } else if (pos2 == length2) {
            highLowContainer.appendCopy(x1.highLowContainer, pos1, length1);
        }
    }

    public static void lazyOr(MutableRoaringBitmap container, ImmutableRoaringBitmap... bitmaps) {
        for (int k = 0; k < bitmaps.length; ++k) {
            container.lazyor(bitmaps[k]);
        }
        container.repairAfterLazy();
    }

    public static void or(MutableRoaringBitmap container, ImmutableRoaringBitmap... bitmaps) {
        if (bitmaps.length == 0) {
            return;
        }

        PriorityQueue<ImmutableRoaringBitmap> pq = new PriorityQueue<>(bitmaps.length, (a, b) -> a.getSizeInBytes() - b.getSizeInBytes());
        Collections.addAll(pq, bitmaps);
        while (pq.size() > 1) {
            ImmutableRoaringBitmap x1 = pq.poll();
            ImmutableRoaringBitmap x2 = pq.poll();
            pq.add(MutableRoaringBitmap.or(x1, x2));
        }
        container.or(pq.poll());
    }

    public static void xor(final MutableRoaringBitmap answer, final MutableRoaringBitmap x1, final MutableRoaringBitmap x2) {
        int pos1 = 0, pos2 = 0;
        final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer.size();

        MutableRoaringArray highLowContainer = (MutableRoaringArray) answer.highLowContainer;

        main:
        if (pos1 < length1 && pos2 < length2) {
            short s1 = x1.highLowContainer.getKeyAtIndex(pos1);
            short s2 = x2.highLowContainer.getKeyAtIndex(pos2);

            while (true) {
                if (s1 < s2) {
                    highLowContainer.appendCopy(x1.highLowContainer.getKeyAtIndex(pos1), x1.highLowContainer.getContainerAtIndex(pos1));
                    pos1++;
                    if (pos1 == length1) {
                        break main;
                    }
                    s1 = x1.highLowContainer.getKeyAtIndex(pos1);
                } else if (s1 > s2) {
                    highLowContainer.appendCopy(x2.highLowContainer.getKeyAtIndex(pos2), x2.highLowContainer.getContainerAtIndex(pos2));
                    pos2++;
                    if (pos2 == length2) {
                        break main;
                    }
                    s2 = x2.highLowContainer.getKeyAtIndex(pos2);
                } else {
                    final MappeableContainer c = x1.highLowContainer.getContainerAtIndex(pos1).xor(
                        x2.highLowContainer.getContainerAtIndex(pos2));
                    if (c.getCardinality() > 0) {
                        highLowContainer.append(s1, c);
                    }
                    pos1++;
                    pos2++;
                    if ((pos1 == length1)
                        || (pos2 == length2)) {
                        break main;
                    }
                    s1 = x1.highLowContainer.getKeyAtIndex(pos1);
                    s2 = x2.highLowContainer.getKeyAtIndex(pos2);
                }
            }
        }
        if (pos1 == length1) {
            highLowContainer.appendCopy(x2.highLowContainer, pos2, length2);
        } else if (pos2 == length2) {
            highLowContainer.appendCopy(x1.highLowContainer, pos1, length1);
        }
    }

    private RoaringBufferAggregation() {
    }

}
