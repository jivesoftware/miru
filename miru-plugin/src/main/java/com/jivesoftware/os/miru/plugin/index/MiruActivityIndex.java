/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

/** @author jonathan */
public interface MiruActivityIndex {

    /**
     * Returns the activity that was recorded at the given index
     *
     * @param tenantId the tenant
     * @param index the index of the activity
     * @return the activity at the given index
     */
    MiruInternalActivity get(String name, MiruTenantId tenantId, int index, StackBuffer stackBuffer) throws IOException, InterruptedException;

    /**
     * Get the terms from the given field for the activity at the requested index.
     *
     * @param index the activity index
     * @param fieldId the field
     * @return the terms
     */
    MiruTermId[] get(String name, int index, int fieldId, StackBuffer stackBuffer) throws IOException, InterruptedException;

    /**
     * Get the terms from the given field for each activity index.
     *
     * @param indexes the activity indexes
     * @param fieldId the field
     * @return the terms
     */
    List<MiruTermId[]> getAll(String name, int[] indexes, int fieldId, StackBuffer stackBuffer) throws IOException, InterruptedException;

    /**
     * Returns the index of the last activity.
     *
     * @return the index of the last activity
     */
    int lastId(StackBuffer stackBuffer);

    /**
     * Store the given activity at the provided index value, and readies the index using the highest id seen.
     * <p/>
     * This method is NOT thread safe.
     *
     * @param activityAndIds the activities to be stored and their indexes
     */
    void setAndReady(MiruSchema schema, Collection<MiruActivityAndId<MiruInternalActivity>> activityAndIds, StackBuffer stackBuffer) throws Exception;

    /**
     * Store the given activity at the provided index value, but does not make ready the new ids.
     * <p/>
     * This method is thread safe.
     *
     * @param activityAndIds the activities to be stored and their indexes
     */
    void set(MiruSchema schema,
        Collection<MiruActivityAndId<MiruInternalActivity>> activityAndIds,
        StackBuffer stackBuffer) throws IOException, InterruptedException;

    /**
     * Readies the index up to the provided index value.
     * <p/>
     * This method is NOT thread safe.
     *
     * @param index the max ready index
     */
    void ready(int index, StackBuffer stackBuffer) throws Exception;

    /** Free resources used by the index. */
    void close();
}
