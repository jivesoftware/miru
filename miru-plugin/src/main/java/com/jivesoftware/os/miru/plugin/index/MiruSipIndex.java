/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.miru.plugin.index;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;

/**
 * @author jonathan
 */
public interface MiruSipIndex<S extends MiruSipCursor<S>> {

    Optional<S> getSip(StackBuffer stackBuffer) throws Exception;

    boolean setSip(S sip, StackBuffer stackBuffer) throws Exception;

    void merge() throws Exception;

    int getRealtimeDeliveryId(StackBuffer stackBuffer) throws Exception;

    boolean setRealtimeDeliveryId(int deliveryId, StackBuffer stackBuffer) throws Exception;
}
