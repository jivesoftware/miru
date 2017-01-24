package com.jivesoftware.os.miru.sync.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.client.aquarium.AmzaClientAquariumProvider;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchemaProvider;
import com.jivesoftware.os.miru.api.sync.MiruSyncClient;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.sync.api.MiruSyncSenderConfig;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.http.client.HttpRequestHelperUtils;
import com.jivesoftware.os.routing.bird.http.client.OAuthSigner;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import oauth.signpost.commonshttp.CommonsHttpOAuthConsumer;
import oauth.signpost.signature.HmacSha1MessageSigner;
import org.apache.commons.lang.StringUtils;

/**
 * Created by jonathan.colt on 12/22/16.
 */
public class MiruSyncSenders<C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Map<String, MiruSyncSender<C, S>> senders = Maps.newConcurrentMap();

    private final MiruStats stats;
    private final MiruSyncConfig syncConfig;
    private final TimestampedOrderIdProvider orderIdProvider;
    private final ScheduledExecutorService executorService;
    private final PartitionClientProvider partitionClientProvider;
    private final AmzaClientAquariumProvider clientAquariumProvider;
    private final ObjectMapper mapper;
    private final MiruSchemaProvider schemaProvider;
    private final MiruSyncSenderConfigProvider syncSenderConfigProvider;
    private final MiruSyncConfigProvider syncConfigProvider;
    private final long ensureSendersInterval;
    private final MiruWALClient<C, S> miruWALClient;
    private final boolean syncLoopback;
    private final MiruSyncClient loopbackSyncClient;
    private final MiruSyncConfigProvider loopbackSyncConfigProvider;
    private final C defaultCursor;
    private final Class<C> cursorClass;

    private final ExecutorService ensureSenders = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setNameFormat("ensure-sender-%d").build());

    private MiruSyncSender<C, S> loopbackSender;

    public MiruSyncSenders(MiruStats stats,
        MiruSyncConfig syncConfig,
        TimestampedOrderIdProvider orderIdProvider,
        ScheduledExecutorService executorService,
        PartitionClientProvider partitionClientProvider,
        AmzaClientAquariumProvider clientAquariumProvider,
        ObjectMapper mapper,
        MiruSchemaProvider schemaProvider,
        MiruSyncSenderConfigProvider syncSenderConfigProvider,
        MiruSyncConfigProvider syncConfigProvider,
        long ensureSendersInterval,
        MiruWALClient<C, S> miruWALClient,
        boolean syncLoopback,
        MiruSyncClient loopbackSyncClient,
        MiruSyncConfigProvider loopbackSyncConfigProvider,
        C defaultCursor,
        Class<C> cursorClass) {

        this.stats = stats;
        this.syncConfig = syncConfig;
        this.orderIdProvider = orderIdProvider;
        this.executorService = executorService;
        this.partitionClientProvider = partitionClientProvider;
        this.clientAquariumProvider = clientAquariumProvider;
        this.mapper = mapper;
        this.schemaProvider = schemaProvider;
        this.syncSenderConfigProvider = syncSenderConfigProvider;
        this.syncConfigProvider = syncConfigProvider;
        this.ensureSendersInterval = ensureSendersInterval;
        this.miruWALClient = miruWALClient;
        this.syncLoopback = syncLoopback;
        this.loopbackSyncClient = loopbackSyncClient;
        this.loopbackSyncConfigProvider = loopbackSyncConfigProvider;
        this.defaultCursor = defaultCursor;
        this.cursorClass = cursorClass;
    }

    public Collection<String> getSyncspaces() {
        return senders.keySet();
    }

    public MiruSyncSender<?, ?> getSender(String syncspaceName) {
        return senders.get(syncspaceName);
    }

    public Collection<MiruSyncSender<C, S>> getActiveSenders() {
        return senders.values();
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            if (syncLoopback) {
                loopbackSender = new MiruSyncSender<>(
                    stats,
                    new MiruSyncSenderConfig("loopback", true, null, null, -1, 10_000L, 60_000L, 10_000, null, null, null, false),
                    clientAquariumProvider,
                    orderIdProvider,
                    syncConfig.getSyncLoopbackRingStripes(),
                    executorService,
                    schemaProvider,
                    miruWALClient,
                    loopbackSyncClient,
                    partitionClientProvider,
                    mapper,
                    loopbackSyncConfigProvider,
                    defaultCursor,
                    cursorClass);
                loopbackSender.start();
            }
            ensureSenders.submit(() -> {
                while (running.get()) {
                    try {
                        Map<String, MiruSyncSenderConfig> all = syncSenderConfigProvider.getAll();
                        for (Entry<String, MiruSyncSenderConfig> entry : all.entrySet()) {
                            String name = entry.getKey();
                            MiruSyncSender<C, S> syncSender = senders.get(name);
                            MiruSyncSenderConfig senderConfig = entry.getValue();
                            if (syncSender != null && syncSender.configHasChanged(senderConfig)) {
                                LOG.info("Restarting sender {} because config has changed", name);
                                syncSender.stop();
                                syncSender = null;
                            }
                            if (syncSender == null) {
                                syncSender = new MiruSyncSender<C, S>(
                                    stats,
                                    senderConfig,
                                    clientAquariumProvider,
                                    orderIdProvider,
                                    syncConfig.getSyncRingStripes(),
                                    executorService,
                                    schemaProvider,
                                    miruWALClient,
                                    syncClient(senderConfig),
                                    partitionClientProvider,
                                    mapper,
                                    syncConfigProvider,
                                    defaultCursor,
                                    cursorClass
                                );

                                senders.put(name, syncSender);
                                syncSender.start();
                            }
                        }

                        // stop any senders that are no longer registered
                        for (Iterator<Entry<String, MiruSyncSender<C, S>>> iterator = senders.entrySet().iterator(); iterator.hasNext(); ) {
                            Entry<String, MiruSyncSender<C, S>> entry = iterator.next();
                            if (!all.containsKey(entry.getKey())) {
                                entry.getValue().stop();
                                iterator.remove();
                            }
                        }

                        Thread.sleep(ensureSendersInterval);
                    } catch (InterruptedException e) {
                        LOG.info("Ensure senders thread {} was interrupted");
                    } catch (Throwable t) {
                        LOG.error("Failure while ensuring senders", t);
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
            if (loopbackSender != null) {
                loopbackSender.stop();
                loopbackSender = null;
            }
        }

        for (MiruSyncSender amzaSyncSender : senders.values()) {
            try {
                amzaSyncSender.stop();
            } catch (Exception x) {
                LOG.warn("Failure while stopping sender:{}", new Object[] { amzaSyncSender }, x);
            }
        }
    }

    private MiruSyncClient syncClient(MiruSyncSenderConfig config) throws Exception {

        String consumerKey = StringUtils.trimToNull(config.oAuthConsumerKey);
        String consumerSecret = StringUtils.trimToNull(config.oAuthConsumerSecret);
        String consumerMethod = StringUtils.trimToNull(config.oAuthConsumerMethod);
        if (consumerKey == null || consumerSecret == null || consumerMethod == null) {
            throw new IllegalStateException("OAuth consumer has not been configured");
        }

        consumerMethod = consumerMethod.toLowerCase();
        if (!consumerMethod.equals("hmac") && !consumerMethod.equals("rsa")) {
            throw new IllegalStateException("OAuth consumer method must be one of HMAC or RSA");
        }

        String scheme = config.senderScheme;
        String host = config.senderHost;
        int port = config.senderPort;

        boolean sslEnable = scheme.equals("https");
        OAuthSigner authSigner = (request) -> {
            CommonsHttpOAuthConsumer oAuthConsumer = new CommonsHttpOAuthConsumer(consumerKey, consumerSecret);
            oAuthConsumer.setMessageSigner(new HmacSha1MessageSigner());
            oAuthConsumer.setTokenWithSecret(consumerKey, consumerSecret);
            return oAuthConsumer.sign(request);
        };
        HttpClient httpClient = HttpRequestHelperUtils.buildHttpClient(sslEnable,
            config.allowSelfSignedCerts,
            authSigner,
            host,
            port,
            syncConfig.getSyncSenderSocketTimeout());

        return new HttpMiruSyncClient(httpClient,
            mapper,
            "/api/sync/v1/write/activities",
            "/api/sync/v1/register/schema");
    }
}
