/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import java.util.List;

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
     * Store the given activity at the provided index value, and readies the index using the highest id seen.
     * <p/>
     * This method is NOT thread safe.
     *
     * @param activityAndIds the activities to be stored and their indexes
     */
    void setAndReady(List<MiruActivityAndId<MiruInternalActivity>> activityAndIds) throws Exception;

    /**
     * Store the given activity at the provided index value, but does not make ready the new ids.
     * <p/>
     * This method is thread safe.
     *
     * @param activityAndIds the activities to be stored and their indexes
     */
    void set(List<MiruActivityAndId<MiruInternalActivity>> activityAndIds);

    /**
     * Readies the index up to the provided index value.
     * <p/>
     * This method is NOT thread safe.
     *
     * @param index the max ready index
     */
    void ready(int index) throws Exception;


    /** Free resources used by the index. */
    void close();
}
