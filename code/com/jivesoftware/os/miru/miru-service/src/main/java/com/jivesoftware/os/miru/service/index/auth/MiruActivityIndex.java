/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.jivesoftware.os.miru.service.index.auth;

import com.jivesoftware.os.miru.api.activity.MiruActivity;

/** @author jonathan */
public interface MiruActivityIndex {

    /**
     * Returns the activity that was recorded at the given index
     *
     * @param index the index of the activity
     * @return the activity at the given index
     */
    MiruActivity get(int index);

    /**
     * Returns the index of the last activity.
     *
     * @return the index of the last activity
     */
    int lastId();

    /**
     * Store the given activity at the provided index value.
     *
     * @param index the index to store the activity at
     * @param activity the activity
     */
    void set(int index, MiruActivity activity);

    /**
     * Total size in bytes of this index in memory.
     *
     * @return total size in bytes
     */
    long sizeInMemory() throws Exception;

    /**
     * Total size in bytes of this index on disk.
     *
     * @return total size in bytes
     */
    long sizeOnDisk() throws Exception;

    /** Free resources used by the index. */
    void close();
}
