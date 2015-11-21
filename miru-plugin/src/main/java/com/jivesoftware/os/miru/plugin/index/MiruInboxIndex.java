/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.miru.api.base.MiruStreamId;

/** @author jonathan */
public interface MiruInboxIndex<IBM> {

    /**
     * Get the inbox index for a given streamId.
     *
     * @param streamId the streamId that represents a given inbox
     * @return the index representing this inbox
     */
    MiruInvertedIndex<IBM> getInbox(MiruStreamId streamId) throws Exception;

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
    int getLastActivityIndex(MiruStreamId streamId, byte[] primitiveBuffer) throws Exception;

    /**
     * Index a given activity id in the inbox matching this streamId. If the inbox doesn't exist it will be created
     * automatically.
     *
     * @param streamId the inbox to index this activity in
     * @param ids      the activity ids to index
     */
    void append(MiruStreamId streamId, byte[] primitiveBuffer, int... ids) throws Exception;

    /** Frees resources used by this index. */
    void close();
}
