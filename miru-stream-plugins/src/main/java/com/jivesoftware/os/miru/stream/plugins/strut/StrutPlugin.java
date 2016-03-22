package com.jivesoftware.os.miru.stream.plugins.strut;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.plugin.MiruEndpointInjectable;
import com.jivesoftware.os.miru.plugin.plugin.MiruPlugin;
import com.jivesoftware.os.miru.plugin.solution.FstRemotePartitionReader;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class StrutPlugin implements MiruPlugin<StrutEndpoints, StrutInjectable> {

    @Override
    public Class<StrutEndpoints> getEndpointsClass() {
        return StrutEndpoints.class;
    }

    @Override
    public Collection<MiruEndpointInjectable<StrutInjectable>> getInjectables(MiruProvider<? extends Miru> miruProvider) {

        StrutConfig config = miruProvider.getConfig(StrutConfig.class);
        TenantAwareHttpClient<String> catwalkHttpClient = miruProvider.getCatwalkHttpClient();

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.registerModule(new GuavaModule());

        HttpResponseMapper responseMapper = new HttpResponseMapper(mapper);

        Cache<String,StrutModel> modelCache = CacheBuilder
            .newBuilder()
            .expireAfterWrite(config.getModelCacheExpirationInMillis(), TimeUnit.MILLISECONDS)
            .maximumSize(config.getModelCacheMaxSize())
            .build();

        StrutModelCache cache = new StrutModelCache(catwalkHttpClient, mapper, responseMapper, modelCache);

        Strut strut = new Strut(cache);
        return Collections.singletonList(new MiruEndpointInjectable<>(
            StrutInjectable.class,
            new StrutInjectable(miruProvider, strut)
        ));
    }

    @Override
    public Collection<MiruRemotePartition<?, ?, ?>> getRemotePartitions(MiruProvider<? extends Miru> miruProvider) {
        FstRemotePartitionReader remotePartitionReader = new FstRemotePartitionReader(miruProvider.getReaderHttpClient(),
            miruProvider.getReaderStrategyCache(),
            false);
        return Arrays.asList(new StrutRemotePartition(remotePartitionReader));
    }
}
