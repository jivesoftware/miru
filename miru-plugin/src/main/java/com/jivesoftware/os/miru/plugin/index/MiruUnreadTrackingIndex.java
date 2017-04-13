package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.base.MiruStreamId;

/**
 *
 * @author jonathan
 */
public interface MiruUnreadTrackingIndex<BM extends IBM, IBM> {

    void setLastActivityIndex(MiruStreamId streamId, int lastActivityIndex, StackBuffer stackBuffer) throws Exception;

    /**
     * Returns the id of the last activity that's been recorded in the unread matching the provided streamId. If
     * the user hasn't requested their unread in a while this might not neccesarily return the latest activity that
     * actually lives in their unread.
     *
     * @param streamId the streamId representing a user's unread
     * @return the id of the latest recorded activity in this unread
     */
    int getLastActivityIndex(MiruStreamId streamId, StackBuffer stackBuffer) throws Exception;

    MiruInvertedIndex<BM, IBM> getUnread(MiruStreamId streamId) throws Exception;

    MiruInvertedIndexAppender getAppender(MiruStreamId streamId) throws Exception;

    void append(MiruStreamId streamId, StackBuffer stackBuffer, int... ids) throws Exception;

    void applyRead(MiruStreamId streamId, IBM readMask, StackBuffer stackBuffer) throws Exception;

    void applyUnread(MiruStreamId streamId, IBM unreadMask, StackBuffer stackBuffer) throws Exception;

    void close() throws Exception;
}
