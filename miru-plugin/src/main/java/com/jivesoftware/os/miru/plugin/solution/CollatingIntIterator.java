package com.jivesoftware.os.miru.plugin.solution;

import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import java.util.NoSuchElementException;

public class CollatingIntIterator {

    private final MiruIntIterator[] iterators;
    private final boolean descending;

    private final int[] values;

    public CollatingIntIterator(MiruIntIterator[] iterators, boolean descending) {
        this.iterators = iterators;
        this.descending = descending;

        this.values = new int[iterators.length];
        for (int i = 0; i < iterators.length; i++) {
            values[i] = -1;
        }
    }

    public boolean hasNext() {
        for (int i = 0; i < values.length; i++) {
            if (values[i] != -1) {
                return true;
            }
        }
        for (MiruIntIterator iterator : iterators) {
            if (iterator != null && iterator.hasNext()) {
                return true;
            }
        }
        return false;
    }

    public int next(boolean[] contained) throws NoSuchElementException {
        int leastIndex = -1;
        int leastObject = -1;
        for (int i = 0; i < values.length; i++) {
            if (values[i] == -1) {
                MiruIntIterator it = iterators[i];
                if (it != null && it.hasNext()) {
                    values[i] = it.next();
                }
            }
            if (values[i] != -1) {
                if (leastIndex == -1) {
                    leastIndex = i;
                    leastObject = values[i];
                } else {
                    int curObject = values[i];
                    if (compare(curObject, leastObject) < 0) {
                        leastObject = curObject;
                        leastIndex = i;
                    }
                }
            }
        }
        for (int i = 0; i < values.length; i++) {
            contained[i] = (values[i] == leastObject);
        }

        if (leastIndex == -1) {
            throw new NoSuchElementException();
        }
        int val = values[leastIndex];
        for (int i = 0; i < values.length; i++) {
            if (values[i] == val) {
                values[i] = -1;
            }
        }
        return val;
    }

    private int compare(int o1, int o2) {
        return descending ? Integer.compare(o2, o1) : Integer.compare(o1, o2);
    }
}
