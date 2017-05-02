package com.jivesoftware.os.miru.siphon.deployable.siphoner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.client.aquarium.AmzaClientAquariumProvider;
import com.jivesoftware.os.aquarium.LivelyEndState;
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.miru.query.siphon.MiruSiphonPlugin;
import com.jivesoftware.os.miru.siphon.deployable.MiruSiphonActivityFlusher;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by jonathan.colt on 4/27/17.
 */
public class AmzaSiphoners {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Map<String, AmzaSiphoner> siphoners = Maps.newConcurrentMap();
    private final ExecutorService ensureSenders = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setNameFormat("ensure-siphoner-%d").build());
    private final PartitionClientProvider partitionClientProvider;
    private final AmzaClientAquariumProvider amzaClientAquariumProvider;
    private final ExecutorService sigonerThreads;

    private final String siphonName;
    private final int siphonStripeCount;
    private final AmzaSiphonerConfigProvider siphonerConfigProvider;
    private final MiruSiphonPluginRegistry siphonPluginRegistry;
    private final MiruSiphonActivityFlusher miruSiphonActivityFlusher;
    private final long ensureSiphonersInterval;
    private Callable<Boolean>[] isStripeElected;
    private final ObjectMapper mapper;

    public AmzaSiphoners(PartitionClientProvider partitionClientProvider,
        AmzaClientAquariumProvider amzaClientAquariumProvider,
        ExecutorService sigonerThreads,
        String siphonName,
        int siphonStripeCount,
        AmzaSiphonerConfigProvider siphonerConfigProvider,
        MiruSiphonPluginRegistry siphonPluginRegistry,
        MiruSiphonActivityFlusher miruSiphonActivityFlusher,
        long ensureSiphonersInterval,
        ObjectMapper mapper) {

        this.partitionClientProvider = partitionClientProvider;
        this.amzaClientAquariumProvider = amzaClientAquariumProvider;
        this.sigonerThreads = sigonerThreads;
        this.siphonName = siphonName;
        this.siphonStripeCount = siphonStripeCount;
        this.siphonerConfigProvider = siphonerConfigProvider;
        this.siphonPluginRegistry = siphonPluginRegistry;
        this.miruSiphonActivityFlusher = miruSiphonActivityFlusher;
        this.ensureSiphonersInterval = ensureSiphonersInterval;
        this.isStripeElected = new Callable[siphonStripeCount];
        this.mapper = mapper;

    }

    public Set<Entry<String, AmzaSiphoner>> all() throws Exception {
        return siphoners.entrySet();
    }

    private String electableStripeName(int siphonStripe) {
        return "miru-siphon-" + siphonName + "-stripe-" + siphonStripe;
    }

    public void start() {
        if (running.compareAndSet(false, true)) {

            for (int i = 0; i < siphonStripeCount; i++) {
                amzaClientAquariumProvider.register(electableStripeName(i));
                final int stripe = i;
                isStripeElected[stripe] = () -> {
                    LivelyEndState livelyEndState = amzaClientAquariumProvider.livelyEndState(electableStripeName(stripe));
                    return livelyEndState != null && livelyEndState.isOnline() && livelyEndState.getCurrentState() == State.leader;
                };
            }

            ensureSenders.submit(() -> {
                while (running.get()) {
                    try {
                        Map<String, AmzaSiphonerConfig> all = siphonerConfigProvider.getAll();
                        for (Entry<String, AmzaSiphonerConfig> entry : all.entrySet()) {
                            AmzaSiphoner siphoner = siphoners.get(entry.getKey());
                            AmzaSiphonerConfig siphonerConfig = entry.getValue();
                            if (siphoner != null && siphoner.configHasChanged(siphonerConfig)) {
                                siphoner.stop();
                                siphoner = null;
                            }

                            if (siphoner == null) {

                                MiruSiphonPlugin miruSiphonPlugin = siphonPluginRegistry.get(siphonerConfig.siphonPluginName);
                                if (miruSiphonPlugin == null) {
                                    LOG.error("No siphon plugin registerd for {}", siphonerConfig.siphonPluginName);
                                    continue;
                                }

                                siphoner = new AmzaSiphoner(siphonerConfig,
                                    miruSiphonPlugin,
                                    siphonerConfig.partitionName,
                                    String.valueOf(siphonerConfig.uniqueId),
                                    siphonerConfig.destinationTenantId,
                                    siphonerConfig.batchSize,
                                    partitionClientProvider,
                                    mapper);

                                siphoners.put(entry.getKey(), siphoner);
                            }

                            if (!siphoner.runnable()) {
                                int stripe = Math.abs(siphonerConfig.partitionName.hashCode()) % siphonStripeCount;
                                if (isStripeElected[stripe].call()) {
                                    LOG.info("Submited SigonRunnable for {}", siphoner);
                                    sigonerThreads.submit(
                                        new SigonRunnable(sigonerThreads, siphonerConfig, siphoner, miruSiphonActivityFlusher, isStripeElected[stripe]));
                                }
                            }
                        }

                        // stop any siphoners that are no longer registered
                        for (Iterator<Entry<String, AmzaSiphoner>> iterator = siphoners.entrySet().iterator(); iterator.hasNext(); ) {
                            Entry<String, AmzaSiphoner> entry = iterator.next();
                            if (!all.containsKey(entry.getKey())) {
                                entry.getValue().stop();
                                iterator.remove();
                            }
                        }

                        Thread.sleep(ensureSiphonersInterval);
                    } catch (InterruptedException e) {
                        LOG.info("Ensure siphoners thread {} was interrupted");
                    } catch (Throwable t) {
                        LOG.error("Failure while ensuring siphoners", t);
                        Thread.sleep(ensureSiphonersInterval);
                    }
                }
                return null;
            });
        }
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            ensureSenders.shutdownNow();
        }

        for (AmzaSiphoner amzaSiphoner : siphoners.values()) {
            try {
                amzaSiphoner.stop();
            } catch (Exception x) {
                LOG.warn("Failure while stopping siphoner:{}", new Object[] { amzaSiphoner }, x);
            }
        }
    }

    private static class SigonRunnable implements Runnable {
        private final ExecutorService threadPool;
        private final AmzaSiphonerConfig siphonerConfig;
        private final AmzaSiphoner siphoner;
        private final MiruSiphonActivityFlusher activityFlusher;
        private final Callable<Boolean> isElected;

        private SigonRunnable(ExecutorService threadPool,
            AmzaSiphonerConfig siphonerConfig,
            AmzaSiphoner siphoner,
            MiruSiphonActivityFlusher activityFlusher,
            Callable<Boolean> isElected) {

            this.threadPool = threadPool;
            this.siphonerConfig = siphonerConfig;
            this.siphoner = siphoner;
            this.activityFlusher = activityFlusher;
            this.isElected = isElected;
        }

        @Override
        public void run() {
            try {
                if (!siphoner.siphon(isElected, activityFlusher)) {
                    threadPool.submit(this);
                }
            } catch (Exception x) {
                LOG.error("Failure while sighoning {}", new Object[] { siphonerConfig }, x);
            }
        }
    }
}
