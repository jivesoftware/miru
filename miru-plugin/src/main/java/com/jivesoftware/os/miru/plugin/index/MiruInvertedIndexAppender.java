package com.jivesoftware.os.miru.plugin.index;

import java.util.List;

public interface MiruInvertedIndexAppender {

    void append(byte[] primitiveBuffer, int... ids) throws Exception;

    void appendAndExtend(List<Integer> ids, int lastId, byte[] primitiveBuffer) throws Exception;
}
