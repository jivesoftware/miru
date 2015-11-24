package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import java.util.List;

public interface MiruInvertedIndexAppender {

    void append(StackBuffer stackBuffer, int... ids) throws Exception;

    void appendAndExtend(List<Integer> ids, int lastId, StackBuffer stackBuffer) throws Exception;
}
