package com.jivesoftware.os.miru.service.index;

import java.util.List;

public interface MiruInvertedIndexAppender {

    void append(int id) throws Exception;

    void appendAndExtend(List<Integer> ids, int lastId) throws Exception;
}
