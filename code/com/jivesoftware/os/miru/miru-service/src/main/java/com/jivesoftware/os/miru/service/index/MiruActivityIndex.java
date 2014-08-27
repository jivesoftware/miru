/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.jivesoftware.os.miru.service.index;

import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.service.activity.MiruInternalActivity;

/** @author jonathan */
public interface MiruActivityIndex {

    /**
     * Returns the activity that was recorded at the given index
     *
     * @param tenantId
     * @param index the index of the activity
     * @return the activity at the given index
     */
    MiruInternalActivity get(MiruTenantId tenantId, int index);

    /**
     *
     * @param index
     * @param fieldId
     * @return
     */
    MiruTermId[] get(MiruTenantId tenantId, int index, int fieldId);

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
    void set(int index, MiruInternalActivity activity);

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
