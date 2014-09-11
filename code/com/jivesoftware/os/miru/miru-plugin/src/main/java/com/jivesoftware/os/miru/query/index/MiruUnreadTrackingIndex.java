package com.jivesoftware.os.miru.query.index;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.base.MiruStreamId;

/**
 *
 * @author jonathan
 */
public interface MiruUnreadTrackingIndex<BM> {

    Optional<BM> getUnread(MiruStreamId streamId) throws Exception;

    MiruInvertedIndexAppender getAppender(MiruStreamId streamId) throws Exception;

    void index(MiruStreamId streamId, int id) throws Exception;

    void applyRead(MiruStreamId streamId, BM readMask) throws Exception;

    void applyUnread(MiruStreamId streamId, BM unreadMask) throws Exception;

    long sizeInMemory() throws Exception;

    long sizeOnDisk() throws Exception;

    void close();
}
