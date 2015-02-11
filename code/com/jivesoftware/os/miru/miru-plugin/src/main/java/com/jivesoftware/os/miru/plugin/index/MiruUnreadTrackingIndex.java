package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.miru.api.base.MiruStreamId;

/**
 *
 * @author jonathan
 */
public interface MiruUnreadTrackingIndex<BM> {

    MiruInvertedIndex<BM> getUnread(MiruStreamId streamId) throws Exception;

    MiruInvertedIndexAppender getAppender(MiruStreamId streamId) throws Exception;

    void append(MiruStreamId streamId, int... ids) throws Exception;

    void applyRead(MiruStreamId streamId, BM readMask) throws Exception;

    void applyUnread(MiruStreamId streamId, BM unreadMask) throws Exception;

    void close();
}
