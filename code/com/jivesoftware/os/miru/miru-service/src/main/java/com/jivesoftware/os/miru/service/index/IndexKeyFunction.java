package com.jivesoftware.os.miru.service.index;

/**
 *
 */
public class IndexKeyFunction {

    // hashes with significantly better distribution for common fieldId, termId pairs using trove's HashFunctions
    public long getKey(int fieldId, int termId) {
        return Long.reverse((long) termId << 32 | fieldId & 0xFFFFFFFFL);
    }
}
