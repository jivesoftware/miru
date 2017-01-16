package com.jivesoftware.os.miru.sync.deployable;

import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.client.http.AmzaClientProvider;
import com.jivesoftware.os.amza.sync.api.AmzaConfigMarshaller;
import com.jivesoftware.os.amza.sync.api.AmzaConfigStorage;
import com.jivesoftware.os.miru.sync.api.MiruSyncSenderConfig;

/**
 *
 */
public class MiruSyncSenderConfigStorage extends AmzaConfigStorage<String, MiruSyncSenderConfig> implements MiruSyncSenderConfigProvider {


    public MiruSyncSenderConfigStorage(AmzaClientProvider clientProvider,
        String partitionName,
        PartitionProperties partitionProperties,
        AmzaConfigMarshaller<String> keyMarshaller,
        AmzaConfigMarshaller<MiruSyncSenderConfig> valueMarshaller) {
        super(clientProvider, partitionName, partitionProperties, keyMarshaller, valueMarshaller);
    }

}
