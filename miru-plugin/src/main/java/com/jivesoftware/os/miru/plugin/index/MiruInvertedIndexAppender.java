package com.jivesoftware.os.miru.plugin.index;

import java.util.List;

public interface MiruInvertedIndexAppender {

    void append(int... ids) throws Exception;

    void appendAndExtend(List<Integer> ids, int lastId) throws Exception;
}
