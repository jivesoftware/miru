package com.jivesoftware.os.wiki.miru.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.RoundRobinStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.wiki.miru.deployable.storage.WikiMiruGramsAmza;
import com.jivesoftware.os.wiki.miru.deployable.storage.WikiMiruPayloadsAmza;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan.colt
 */
public class WikiMiruStressService {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public final AtomicLong indexed = new AtomicLong();
    private final OrderIdProvider idProvider;
    private final WikiSchemaService wikiSchemaService;
    private final String miruIngressEndpoint;
    private final ObjectMapper activityMapper;
    private final TenantAwareHttpClient<String> miruWriter;
    private final WikiMiruPayloadsAmza payloads;
    private final WikiMiruGramsAmza wikiMiruGramsAmza;
    private final RoundRobinStrategy roundRobinStrategy = new RoundRobinStrategy();

    public WikiMiruStressService(OrderIdProvider idProvider,
        WikiSchemaService wikiSchemaService,
        String miruIngressEndpoint,
        ObjectMapper activityMapper,
        TenantAwareHttpClient<String> miruWriter,
        WikiMiruPayloadsAmza payloads, WikiMiruGramsAmza wikiMiruGramsAmza) {
        this.idProvider = idProvider;
        this.wikiSchemaService = wikiSchemaService;
        this.miruIngressEndpoint = miruIngressEndpoint;
        this.activityMapper = activityMapper;
        this.miruWriter = miruWriter;
        this.payloads = payloads;
        this.wikiMiruGramsAmza = wikiMiruGramsAmza;
    }

    public Stresser stress(String tenantId, int qps) throws Exception {

        return new Stresser(String.valueOf(idProvider.nextId()), tenantId, qps);

    }

    public class Stresser {

        public final String stresserId;
        public final String tenantId;
        private final int qps;
        public final AtomicLong queried = new AtomicLong();
        public final AtomicBoolean running = new AtomicBoolean(true);
        public final long startTimestampMillis = System.currentTimeMillis();
        public String message = "";


        public Stresser(String stresserId, String tenantId, int qps) throws NoSuchAlgorithmException {
            this.stresserId = stresserId;
            this.tenantId = tenantId;
            this.qps = qps;

        }


        public void start() throws Exception {
            try {
                message = "starting";
                while(running.get()) {
                    try {

                    } catch(Exception x) {

                    }
                }
            } finally {
                message = "done";
                running.set(false);
            }
        }
    }



}
