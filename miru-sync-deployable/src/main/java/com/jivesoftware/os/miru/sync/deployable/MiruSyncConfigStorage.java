package com.jivesoftware.os.miru.sync.deployable;

import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.client.http.AmzaClientProvider;
import com.jivesoftware.os.amza.sync.api.AmzaConfigMarshaller;
import com.jivesoftware.os.amza.sync.api.AmzaPartitionedConfigStorage;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.sync.api.MiruSyncTenantConfig;

/**
 *
 */
public class MiruSyncConfigStorage extends AmzaPartitionedConfigStorage<MiruTenantId, MiruSyncTenantConfig> implements MiruSyncConfigProvider {
    public MiruSyncConfigStorage(AmzaClientProvider clientProvider,
        String partitionName,
        PartitionProperties partitionProperties,
        AmzaConfigMarshaller<MiruTenantId> keyMarshaller,
        AmzaConfigMarshaller<MiruSyncTenantConfig> valueMarshaller) {
        super(clientProvider, partitionName, partitionProperties, keyMarshaller, valueMarshaller);
    }


}
