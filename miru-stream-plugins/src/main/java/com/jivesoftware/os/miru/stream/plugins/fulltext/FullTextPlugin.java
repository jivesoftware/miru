package com.jivesoftware.os.miru.stream.plugins.fulltext;

import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.plugin.MiruEndpointInjectable;
import com.jivesoftware.os.miru.plugin.plugin.MiruPlugin;
import com.jivesoftware.os.miru.plugin.solution.JsonRemotePartitionReader;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartitionReader;
import com.jivesoftware.os.miru.plugin.solution.SnappyJsonRemotePartitionReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 *
 */
public class FullTextPlugin implements MiruPlugin<FullTextEndpoints, FullTextInjectable> {

    @Override
    public Class<FullTextEndpoints> getEndpointsClass() {
        return FullTextEndpoints.class;
    }

    @Override
    public Collection<MiruEndpointInjectable<FullTextInjectable>> getInjectables(MiruProvider<? extends Miru> miruProvider) {

        FullText fullText = new FullText(miruProvider);
        return Collections.singletonList(new MiruEndpointInjectable<>(
            FullTextInjectable.class,
            new FullTextInjectable(miruProvider, fullText)
        ));
    }

    @Override
    public Collection<MiruRemotePartition<?, ?, ?>> getRemotePartitions(MiruProvider<? extends Miru> miruProvider) {
        FullTextConfig config = miruProvider.getConfig(FullTextConfig.class);
        MiruRemotePartitionReader remotePartitionReader;
        if (config.getRemoteSnappyCompression()) {
            remotePartitionReader = new SnappyJsonRemotePartitionReader(miruProvider.getReaderHttpClient(),
                miruProvider.getReaderStrategyCache());
        } else {
            remotePartitionReader = new JsonRemotePartitionReader(miruProvider.getReaderHttpClient(),
                miruProvider.getReaderStrategyCache());
        }
        return Arrays.asList(new FullTextCustomRemotePartition(remotePartitionReader));
    }
}
