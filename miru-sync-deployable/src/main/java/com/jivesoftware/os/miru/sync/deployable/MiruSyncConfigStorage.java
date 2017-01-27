package com.jivesoftware.os.miru.sync.deployable;

import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.client.collection.AmzaMarshaller;
import com.jivesoftware.os.amza.client.http.AmzaClientProvider;
import com.jivesoftware.os.amza.sync.api.AmzaPartitionedConfigStorage;
import com.jivesoftware.os.miru.sync.api.MiruSyncTenantConfig;
import com.jivesoftware.os.miru.sync.api.MiruSyncTenantTuple;

/**
 *
 */
public class MiruSyncConfigStorage extends AmzaPartitionedConfigStorage<MiruSyncTenantTuple, MiruSyncTenantConfig> implements MiruSyncConfigProvider {
    public MiruSyncConfigStorage(AmzaClientProvider clientProvider,
        String partitionName,
        PartitionProperties partitionProperties,
        AmzaMarshaller<MiruSyncTenantTuple> keyMarshaller,
        AmzaMarshaller<MiruSyncTenantConfig> valueMarshaller) {
        super(clientProvider, partitionName, partitionProperties, keyMarshaller, valueMarshaller);
    }


}
