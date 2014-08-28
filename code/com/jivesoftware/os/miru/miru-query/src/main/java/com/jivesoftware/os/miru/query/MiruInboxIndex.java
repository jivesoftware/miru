/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.jivesoftware.os.miru.query;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.base.MiruStreamId;

/** @author jonathan */
public interface MiruInboxIndex<BM> {

    /**
     * Get the inbox index for a given streamId. If the inbox doesn't exist then we will
     * return Optional.absent();
     *
     * @param streamId the streamId that represents a given inbox
     * @return the index representing this inbox or Optional.absent() if it doesn't exist
     */
    Optional<BM> getInbox(MiruStreamId streamId) throws Exception;

    /**
     * Get the inbox index for a given streamId, creating if it doesn't already exist.
     *
     * @param streamId the streamId that represents a given inbox
     * @return the index representing this inbox
     */
    MiruInvertedIndexAppender getAppender(MiruStreamId streamId) throws Exception;

    /**
     * Returns the id of the last activity that's been recorded in the inbox matching the provided streamId. If
     * the user hasn't requested their inbox in a while this might not neccesarily return the latest activity that
     * actually lives in their inbox.
     *
     * @param streamId the streamId representing a user's inbox
     * @return the id of the latest recorded activity in this inbox
     */
    int getLastActivityIndex(MiruStreamId streamId) throws Exception;

    /**
     * Index a given activity id in the inbox matching this streamId. If the inbox doesn't exist it will be created
     * automatically.
     *
     * @param streamId the inbox to index this activity in
     * @param id the activity id to index
     */
    void index(MiruStreamId streamId, int id) throws Exception;

    /**
     * Set the index of the last activity id in the inbox matching the provided streamId
     *
     * @param streamId the inbox where this activity lives
     * @param activityIndex the activity id to record
     */
    void setLastActivityIndex(MiruStreamId streamId, int activityIndex) throws Exception;

    /**
     * Returns the index size in bytes in memory.
     *
     * @return the size in bytes
     */
    long sizeInMemory() throws Exception;

    /**
     * Returns the index size in bytes on disk.
     *
     * @return the size in bytes
     */
    long sizeOnDisk() throws Exception;

    /** Frees resources used by this index. */
    void close();
}
