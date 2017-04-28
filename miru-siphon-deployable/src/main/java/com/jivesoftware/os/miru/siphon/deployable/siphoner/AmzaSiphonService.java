package com.jivesoftware.os.miru.siphon.deployable.siphoner;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by jonathan.colt on 4/27/17.
 */
public class AmzaSiphonService {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Map<String, AmzaSiphoner> siphoners = Maps.newConcurrentMap();
    private final ExecutorService ensureSenders = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setNameFormat("ensure-siphoner-%d").build());


    private final AmzaSiphonerConfigProvider siphonerConfigProvider;
    private final long ensureSendersInterval;

    public AmzaSiphonService(AmzaSiphonerConfigProvider siphonerConfigProvider, long ensureSendersInterval) {
        this.siphonerConfigProvider = siphonerConfigProvider;
        this.ensureSendersInterval = ensureSendersInterval;
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            ensureSenders.submit(() -> {
                while (running.get()) {
                    try {
                        Map<String, AmzaSiphonerConfig> all = siphonerConfigProvider.getAll();
                        for (Entry<String, AmzaSiphonerConfig> entry : all.entrySet()) {
                            AmzaSiphoner amzaSiphoner = siphoners.get(entry.getKey());
                            AmzaSiphonerConfig siphonerConfig = entry.getValue();
                            if (amzaSiphoner != null && amzaSiphoner.configHasChanged(siphonerConfig)) {
                                amzaSiphoner.stop();
                                amzaSiphoner = null;
                            }
                            if (amzaSiphoner == null) {

                                amzaSiphoner = null; //new AmzaSiphoner(

                                /*amzaSyncSender = new AmzaSiphoner(
                                    stats,
                                    siphonerConfig,
                                    clientAquariumProvider,
                                    syncConfig.getSyncSenderRingStripes(),
                                    executorService,
                                    partitionClientProvider,
                                    amzaSyncClient(siphonerConfig),
                                    syncPartitionConfigProvider,
                                    amzaInterner
                                );*/

                                siphoners.put(entry.getKey(), amzaSiphoner);
                                amzaSiphoner.start();
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

                        Thread.sleep(ensureSendersInterval);
                    } catch (InterruptedException e) {
                        LOG.info("Ensure siphoners thread {} was interrupted");
                    } catch (Throwable t) {
                        LOG.error("Failure while ensuring siphoners", t);
                        Thread.sleep(ensureSendersInterval);
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

}
