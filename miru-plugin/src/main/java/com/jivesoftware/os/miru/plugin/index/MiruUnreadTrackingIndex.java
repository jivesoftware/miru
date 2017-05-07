package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.topology.NamedCursor;
import java.util.List;

/**
 *
 * @author jonathan
 */
public interface MiruUnreadTrackingIndex<BM extends IBM, IBM> {

    void setLastActivityIndex(MiruStreamId streamId, int lastActivityIndex, StackBuffer stackBuffer) throws Exception;

    int getLastActivityIndex(MiruStreamId streamId, StackBuffer stackBuffer) throws Exception;

    void setCursors(MiruStreamId streamId, List<NamedCursor> cursors) throws Exception;

    List<NamedCursor> getCursors(MiruStreamId streamId) throws Exception;

    MiruInvertedIndex<BM, IBM> getUnread(MiruStreamId streamId) throws Exception;

    MiruInvertedIndexAppender getAppender(MiruStreamId streamId) throws Exception;

    void append(MiruStreamId streamId, StackBuffer stackBuffer, int... ids) throws Exception;

    void applyRead(MiruStreamId streamId, IBM readMask, StackBuffer stackBuffer) throws Exception;

    void applyUnread(MiruStreamId streamId, IBM unreadMask, StackBuffer stackBuffer) throws Exception;

    void close() throws Exception;
}
