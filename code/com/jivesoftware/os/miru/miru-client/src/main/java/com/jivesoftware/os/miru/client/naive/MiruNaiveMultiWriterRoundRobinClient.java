package com.jivesoftware.os.miru.client.naive;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruReadEvent;
import com.jivesoftware.os.miru.client.MiruClient;
import com.jivesoftware.os.miru.client.MiruClientConfig;
import com.jivesoftware.os.miru.client.MiruClientInitializer;
import com.jivesoftware.os.miru.client.base.MiruBestEffortFailureTolerantClient;
import com.jivesoftware.os.miru.cluster.MiruRegistryInitializer;
import com.jivesoftware.os.miru.wal.MiruWALInitializer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 *
 */
@Singleton
public class MiruNaiveMultiWriterRoundRobinClient implements MiruClient {

    private final List<MiruClient> miruClients = Lists.newArrayList();
    private final AtomicInteger roundRobinCounter = new AtomicInteger();

    @Inject
    public MiruNaiveMultiWriterRoundRobinClient(MiruClientConfig miruClientConfig,
        MiruRegistryInitializer miruRegistryInitializer,
        MiruWALInitializer miruWALInitializer) throws Exception {

        Optional<Class<? extends MiruClient>> clientClass = Optional.<Class<? extends MiruClient>>of(MiruBestEffortFailureTolerantClient.class);

        for (int writerId : getWriterIds(miruClientConfig)) {
            miruClients.add(MiruClientInitializer.initialize(miruClientConfig,
                miruRegistryInitializer, miruWALInitializer,
                writerId, clientClass));
        }
    }

    public List<Integer> getWriterIds(MiruClientConfig miruClientConfig) {
        return Lists.transform(Lists.newArrayList(miruClientConfig.getWriterIds().split("\\s*,\\s*")), new Function<String, Integer>() {
            @Nullable
            @Override
            public Integer apply(@Nullable String input) {
                return new Integer(input);
            }
        });
    }

    @Override
    public void sendActivity(List<MiruActivity> activities, boolean recoverFromRemoval) throws Exception {
        nextClient().sendActivity(activities, recoverFromRemoval);
    }

    @Override
    public void removeActivity(List<MiruActivity> activities) throws Exception {
        nextClient().removeActivity(activities);
    }

    @Override
    public void sendRead(MiruReadEvent readEvent) throws Exception {
        nextClient().sendRead(readEvent);
    }

    @Override
    public void sendUnread(MiruReadEvent readEvent) throws Exception {
        nextClient().sendUnread(readEvent);
    }

    @Override
    public void sendAllRead(MiruReadEvent readEvent) throws Exception {
        nextClient().sendAllRead(readEvent);
    }

    private MiruClient nextClient() {
        return miruClients.get(roundRobinCounter.getAndIncrement() % miruClients.size());
    }
}
