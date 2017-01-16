package com.jivesoftware.os.miru.sync.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.client.aquarium.AmzaClientAquariumProvider;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchemaProvider;
import com.jivesoftware.os.miru.api.sync.MiruSyncClient;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.sync.api.MiruSyncSenderConfig;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpRequestHelper;
import com.jivesoftware.os.routing.bird.http.client.HttpRequestHelperUtils;
import com.jivesoftware.os.routing.bird.http.client.OAuthSigner;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

    private final MiruSyncConfig syncConfig;
    private final TimestampedOrderIdProvider orderIdProvider;
    private final ExecutorService executorService;
    private final PartitionClientProvider partitionClientProvider;
    private final AmzaClientAquariumProvider clientAquariumProvider;
    private final ObjectMapper mapper;
    private final MiruSchemaProvider schemaProvider;
    private final MiruSyncSenderConfigProvider syncSenderConfigProvider;
    private final MiruSyncConfigProvider syncConfigProvider;
    private final long ensureSendersInterval;
    private final MiruWALClient<C, S> miruWALClient;
    private final C defaultCursor;
    private final Class<C> cursorClass;

    private final ExecutorService ensureSenders = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setNameFormat("ensure-sender-%d").build());

    public MiruSyncSenders(MiruSyncConfig syncConfig,
        TimestampedOrderIdProvider orderIdProvider,
        ExecutorService executorService,
        PartitionClientProvider partitionClientProvider,
        AmzaClientAquariumProvider clientAquariumProvider,
        ObjectMapper mapper,
        MiruSchemaProvider schemaProvider,
        MiruSyncSenderConfigProvider syncSenderConfigProvider,
        MiruSyncConfigProvider syncConfigProvider,
        long ensureSendersInterval,
        MiruWALClient<C, S> miruWALClient,
        C defaultCursor,
        Class<C> cursorClass
    ) {
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
        this.defaultCursor = defaultCursor;
        this.cursorClass = cursorClass;
    }

    public Collection<MiruSyncSender<C, S>> getActiveSenders() {
        return senders.values();
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            ensureSenders.submit(() -> {
                while (running.get()) {
                    try {
                        Map<String, MiruSyncSenderConfig> all = syncSenderConfigProvider.getAll();
                        for (Entry<String, MiruSyncSenderConfig> entry : all.entrySet()) {
                            MiruSyncSender<C, S> syncSender = senders.get(entry.getKey());
                            if (syncSender != null) {
                                // TODO see if config changes and if so teardown and restart?
                            } else {

                                MiruSyncSenderConfig senderConfig = entry.getValue();

                                syncSender = new MiruSyncSender<C, S>(
                                    entry.getKey(),
                                    clientAquariumProvider,
                                    orderIdProvider,
                                    syncConfig.getSyncRingStripes(),
                                    executorService,
                                    syncConfig.getSyncThreadCount(),
                                    senderConfig.syncIntervalMillis,
                                    schemaProvider,
                                    miruWALClient,
                                    syncClient(senderConfig),
                                    partitionClientProvider,
                                    mapper,
                                    syncConfigProvider,
                                    senderConfig.batchSize,
                                    senderConfig.forwardSyncDelayMillis,
                                    defaultCursor,
                                    cursorClass
                                );

                                senders.put(entry.getKey(), syncSender);
                                syncSender.start();
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
        }

        for (MiruSyncSender amzaSyncSender : senders.values()) {
            try {
                amzaSyncSender.stop();
            } catch (Exception x) {
                LOG.warn("Failure while stopping sender:{}", new Object[] { amzaSyncSender }, x);
            }
        }
    }

    public MiruSyncClient syncClient(MiruSyncSenderConfig config) throws Exception {

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
        HttpRequestHelper requestHelper = HttpRequestHelperUtils.buildRequestHelper(sslEnable,
            config.allowSelfSignedCerts,
            authSigner,
            host,
            port,
            syncConfig.getSyncSenderSocketTimeout());

        return new HttpSyncClient(requestHelper,
            "/api/sync/v1/write/activities",
            "/api/sync/v1/write/reads",
            "/api/sync/v1/register/schema");
    }
}
