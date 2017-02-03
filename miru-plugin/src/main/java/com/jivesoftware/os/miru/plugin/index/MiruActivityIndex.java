/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import java.util.Collection;

/** @author jonathan */
public interface MiruActivityIndex {

    /**
     * Returns the time and version that was recorded at the given index
     *
     * @param index the index of the activity
     * @return the time and version at the given index
     */
    TimeVersionRealtime getTimeVersionRealtime(String name, int index, StackBuffer stackBuffer) throws Exception;

    /**
     * Returns the times and versions that were recorded at the given indexes
     *
     * @param indexes the indexes of the activity
     * @return the times and versions at the given indexes
     */
    TimeVersionRealtime[] getAllTimeVersionRealtime(String name, int[] indexes, StackBuffer stackBuffer) throws Exception;

    /**
     * Stream all times and versions
     *
     * @param stream the stream
     * @return whether the stream completed
     */
    boolean streamTimeVersionRealtime(StackBuffer stackBuffer, TimeVersionRealtimeStream stream) throws Exception;

    interface TimeVersionRealtimeStream {

        boolean stream(int id, long timestamp, long version, long monoTimestamp, boolean realtimeDelivery) throws Exception;
    }

    /**
     * Get the terms from the given field for the activity at the requested index.
     *
     * @param index the activity index
     * @param fieldDefinition the field
     * @return the terms
     */
    MiruTermId[] get(String name, int index, MiruFieldDefinition fieldDefinition, StackBuffer stackBuffer) throws Exception;

    /**
     * Get the terms from the given field for each activity index.
     *
     * @param indexes the activity indexes
     * @param fieldDefinition the field
     * @return the terms
     */
    MiruTermId[][] getAll(String name, int[] indexes, MiruFieldDefinition fieldDefinition, StackBuffer stackBuffer) throws Exception;

    /**
     * Get the values from the given property for the activity at the requested index.
     *
     * @param index the activity index
     * @param propId the property
     * @return the terms
     */
    MiruIBA[] getProp(String name, int index, int propId, StackBuffer stackBuffer);

    /**
     * Get the authz for the activity at the requested index.
     *
     * @param index the activity index
     * @return the terms
     */
    String[] getAuthz(String name, int index, StackBuffer stackBuffer);

    /**
     * Get the terms from the given field for each activity index.
     *
     * @param indexes the activity indexes
     * @param offset the offset into indexes
     * @param length the length of indexes
     * @param fieldDefinition the field
     * @return the terms
     */
    MiruTermId[][] getAll(String name,
        int[] indexes,
        int offset,
        int length,
        MiruFieldDefinition fieldDefinition,
        StackBuffer stackBuffer) throws Exception;

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
        StackBuffer stackBuffer) throws Exception;

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
