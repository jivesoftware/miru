package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.miru.api.base.MiruStreamId;

/**
 *
 * @author jonathan
 */
public interface MiruUnreadTrackingIndex<IBM> {

    MiruInvertedIndex<IBM> getUnread(MiruStreamId streamId) throws Exception;

    MiruInvertedIndexAppender getAppender(MiruStreamId streamId) throws Exception;

    void append(MiruStreamId streamId, byte[] primitiveBuffer, int... ids) throws Exception;

    void applyRead(MiruStreamId streamId, IBM readMask, byte[] primitiveBuffer) throws Exception;

    void applyUnread(MiruStreamId streamId, IBM unreadMask, byte[] primitiveBuffer) throws Exception;

    void close();
}
