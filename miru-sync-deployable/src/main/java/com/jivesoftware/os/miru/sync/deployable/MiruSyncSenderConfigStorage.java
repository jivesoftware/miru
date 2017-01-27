package com.jivesoftware.os.miru.sync.deployable;

import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.client.collection.AmzaMap;
import com.jivesoftware.os.amza.client.collection.AmzaMarshaller;
import com.jivesoftware.os.amza.client.http.AmzaClientProvider;
import com.jivesoftware.os.miru.sync.api.MiruSyncSenderConfig;

/**
 *
 */
public class MiruSyncSenderConfigStorage extends AmzaMap<String, MiruSyncSenderConfig> implements MiruSyncSenderConfigProvider {


    public MiruSyncSenderConfigStorage(AmzaClientProvider clientProvider,
        String partitionName,
        PartitionProperties partitionProperties,
        AmzaMarshaller<String> keyMarshaller,
        AmzaMarshaller<MiruSyncSenderConfig> valueMarshaller) {
        super(clientProvider, partitionName, partitionProperties, keyMarshaller, valueMarshaller);
    }

}
