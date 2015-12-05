package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.base.MiruStreamId;

/**
 *
 * @author jonathan
 */
public interface MiruUnreadTrackingIndex<BM extends IBM, IBM> {

    MiruInvertedIndex<BM, IBM> getUnread(MiruStreamId streamId) throws Exception;

    MiruInvertedIndexAppender getAppender(MiruStreamId streamId) throws Exception;

    void append(MiruStreamId streamId, StackBuffer stackBuffer, int... ids) throws Exception;

    void applyRead(MiruStreamId streamId, IBM readMask, StackBuffer stackBuffer) throws Exception;

    void applyUnread(MiruStreamId streamId, IBM unreadMask, StackBuffer stackBuffer) throws Exception;

    void close();
}
