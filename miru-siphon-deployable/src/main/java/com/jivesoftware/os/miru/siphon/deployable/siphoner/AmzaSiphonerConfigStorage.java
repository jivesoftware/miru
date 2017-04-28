package com.jivesoftware.os.miru.siphon.deployable.siphoner;

import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.client.collection.AmzaMap;
import com.jivesoftware.os.amza.client.collection.AmzaMarshaller;
import com.jivesoftware.os.amza.client.http.AmzaClientProvider;

/**
 * Created by jonathan.colt on 4/27/17.
 */
public class AmzaSiphonerConfigStorage extends AmzaMap<String, AmzaSiphonerConfig> implements AmzaSiphonerConfigProvider {

    public AmzaSiphonerConfigStorage(AmzaClientProvider clientProvider,
        String partitionName,
        PartitionProperties partitionProperties,
        AmzaMarshaller<String> keyMarshaller,
        AmzaMarshaller<AmzaSiphonerConfig> valueMarshaller) {
        super(clientProvider,partitionName,partitionProperties,keyMarshaller,valueMarshaller);
    }
}
