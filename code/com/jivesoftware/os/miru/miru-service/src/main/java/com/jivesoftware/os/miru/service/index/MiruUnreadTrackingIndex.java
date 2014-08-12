package com.jivesoftware.os.miru.service.index;

import com.google.common.base.Optional;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.miru.api.base.MiruStreamId;

/**
 *
 * @author jonathan
 */
public interface MiruUnreadTrackingIndex {

    Optional<EWAHCompressedBitmap> getUnread(MiruStreamId streamId) throws Exception;

    MiruInvertedIndexAppender getAppender(MiruStreamId streamId) throws Exception;

    void index(MiruStreamId streamId, int id) throws Exception;

    void applyRead(MiruStreamId streamId, EWAHCompressedBitmap readMask) throws Exception;

    void applyUnread(MiruStreamId streamId, EWAHCompressedBitmap unreadMask) throws Exception;

    long sizeInMemory() throws Exception;

    long sizeOnDisk() throws Exception;

    void close();
}
