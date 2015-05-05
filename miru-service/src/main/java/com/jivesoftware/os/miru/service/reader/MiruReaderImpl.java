package com.jivesoftware.os.miru.service.reader;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.MiruReader;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.service.MiruService;

/**
 *
 */
public class MiruReaderImpl implements MiruReader {

    private final MiruService miruService;

    public MiruReaderImpl(MiruService miruService) {
        this.miruService = miruService;
    }

    @Override
    public <P, R> R read(MiruTenantId tenantId, Optional<MiruActorId> actorId, P params, String endpoint, Class<R> resultClass, R defaultResult)
            throws MiruQueryServiceException {
        throw new UnsupportedOperationException("No reading with this reader");
    }

    @Override
    public void warm(MiruTenantId tenantId) throws MiruQueryServiceException {
        try {
            miruService.warm(tenantId);
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to warm", e);
        }
    }

}
