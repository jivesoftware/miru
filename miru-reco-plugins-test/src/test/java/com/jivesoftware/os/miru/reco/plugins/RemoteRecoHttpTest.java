package com.jivesoftware.os.miru.reco.plugins;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsQuery;
import com.jivesoftware.os.miru.reco.plugins.reco.RecoAnswer;
import com.jivesoftware.os.miru.reco.plugins.reco.RecoConstants;
import com.jivesoftware.os.miru.reco.plugins.reco.RecoQuery;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingAnswer;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingConstants;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingQuery;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingQueryScoreSet;
import com.jivesoftware.os.routing.bird.http.client.HttpClientConfiguration;
import com.jivesoftware.os.routing.bird.http.client.HttpClientFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.routing.bird.http.client.HttpRequestHelper;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.Charsets;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;

/**
 *
 */
public class RemoteRecoHttpTest {

    private static final String[] REMOTE_HOSTS = new String[] {
        "soa-prime-data8.phx1.jivehosted.com"
    };
    private static final int REMOTE_PORT = 10_004;

    final String[] tenants = new String[] {
        "EVy", "Nv9", "KJt", "WVB", "ZlR", "iXM", "lPm", "Z49", "nyc", "oFd", "yso", "1MO", "40M", "2RV", "999", "EVy", "Nv9", "KJt", "WVB", "ZlR",
        "iXM", "lPm", "Z49", "nyc", "yso", "1MO", "9yG", "999", "EVy", "Nv9", "KJt"
    };
    /*
    final String[] tenants = new String[] {
        "fsz", "TWG", "eD6", "Tcb", "3vU", "Hh9", "tw2", "BQs", "2L5", "Vub", "59t", "ZlR", "ueW", "AQ3", "xEK", "wkV", "2xQ", "7Bo", "XZe", "DQP", "MU6",
        "xwj", "vmJ", "HkQ", "9yG", "1Jv", "6Pb", "rDB", "PXz", "yvu", "MIF", "g52", "MZf", "6Vs", "yox", "EIz", "CoH", "a0s", "4tM", "611", "Tko", "9qQ",
        "aDC", "kfQ", "lec", "KfA", "zpS", "2Fc", "5Tu", "nGu", "ZwP", "HGx", "LDw", "egV", "U7A", "NcL", "aLc", "2B8", "1eK", "nLv", "ZqU", "1MO", "NFy",
        "UNG", "ioa", "bv8", "4CV", "i0z", "Wjo", "MEg", "qm8", "0Og", "2rB", "Y0z", "2zG", "j7R", "zGx", "Goc", "2bH", "0i1", "Lzp", "1To", "XFW", "9xi",
        "7v1", "UZE", "48z", "1nF", "qID", "LLf", "h8A", "M5S", "AE9", "RKI", "kWi", "IW0", "NCW", "Nfn", "kU1", "63s", "0hw", "E0y", "SGl", "r9S", "1lX",
        "qbH", "jN3", "8lf", "08Y", "4RC", "5nF", "24l", "125", "hVN", "069", "wfE", "0dk", "gq8", "rfn", "FIm", "0r9", "sxh", "NnN", "oiE", "nCh", "ujR",
        "1bV", "Zll", "unp", "oMh", "CVZ", "0aP", "UwF", "ls6", "fKq", "TyL", "PX7", "Je8", "xMA", "LKO", "rEn", "lwp", "z8E", "tqb", "ard", "3zE", "Knn",
        "YRz", "qPZ", "sZj", "hgj", "BMd", "dDP", "qHY", "dRe", "sig", "lKR", "nEc", "EBD", "7Rq", "jwk", "999", "6vj", "NPO", "fZw", "yNW", "pHU", "LHC",
        "ZaQ", "ySc", "0z4", "MT5", "uX6", "126", "OY1", "G2q", "08V", "3Zv", "GZ5", "5g9", "W03", "RiR", "354", "jGd", "94c", "tu5", "Qy6", "B5Y", "4F1",
        "Op7", "NSe", "hou", "Tu0", "2U7", "GKF", "9U5", "Nsu", "WhE", "qZP", "9Ic", "9H5", "9JZ", "dfM", "anl", "cyp", "EnH", "MFm", "0pq", "XxH", "UMB",
        "USw", "cjL", "SR6", "IfR", "GbP", "ikO", "TWv", "fU5", "TW7", "oFd", "abX", "4PO", "pyH", "y7M", "lPm", "1Bm", "j1X", "Z6Z", "0r5", "3zQ", "JqO",
        "rVV", "zin", "Zeq", "KM7", "0ww", "SAI", "H6t", "jZZ", "2Vp", "04j", "BVR", "pWM", "8U6", "G5q", "cAN", "yso", "eox", "fkP", "QRw", "leG", "P1t",
        "Tqr", "CQF", "yp1", "0hR", "3B8", "4KF", "CLZ", "Pb8", "PLv", "X0F", "bIE", "0Us", "ati", "ITB", "z3G", "FZ2", "bnd", "kgr", "uGE", "yay", "AZC",
        "H4a", "EzS", "74R", "cPs", "k4s", "LrM", "638", "MQu", "OgF", "wmn", "ukh", "xeF", "NLh", "5vT", "0r4", "4lr", "h0j", "IWO", "U81", "Iyl", "xvf",
        "2Lo", "9o2", "M7u", "UYp", "uoT", "lcZ", "WZS", "aAr", "dEt", "xsP", "sEH", "vYr", "0sf", "tqF", "Bcc", "gUT", "tCr", "6Sn", "Bqe", "7pc", "iXM",
        "GlG", "5Ia", "MWL", "o4X", "46q", "FXp", "WjE", "T3C", "3ZS", "DhY", "bUz", "AxI", "bUa", "mtz", "0JN", "Enp", "BEu", "M7D", "ltD", "UFS", "Lgx",
        "2Vm", "3rF", "9l0", "2nz", "zlK", "2RV", "go1", "BaV", "2fA", "6iT", "rZs", "Nyb", "w9Y", "NPb", "zT7", "7FF", "Wsr", "Ft2", "1ak", "paL", "3PE",
        "h3n", "6VO", "2lW", "j0k", "Hz9", "1Vx", "8kj", "sxW", "ZAa", "ZhK", "SGU", "5mX", "2vK", "kFY", "J1C", "G8p", "61q", "vrR", "Ebo", "FZa", "1h7",
        "2rH", "bE1", "KDH", "bEl", "0eB", "KzG", "las", "FFZ"
    };
    */
    /*
    final String[] tenants = new String[] { "999" }; //brewspace
    */
    final int[] userIds = new int[] { 3765, 2902, 3251, 3080, 6668, 8190, 5786, 3957, 3585, 5675, 2112, 3183, 4699, 3624, 5710, 7195, 8433, 5760, 5648,
        8619, 5752, 3384, 2858, 2885, 2021, 8296, 5372, 3251, 2873, 8417, 5571, 7841, 8419, 3763, 4473, 2002, 4177, 5446, 2035, 5514, 8217 };
    //String tenant = "Z49"; //big tenant

    @Test(enabled = false, description = "Needs REMOTE constants")
    public void testSystemTrending() throws Exception {

        //MiruTenantId tenantId = new MiruTenantId(tenant.getBytes(Charsets.UTF_8));

        HttpClientFactory httpClientFactory = new HttpClientFactoryProvider()
            .createHttpClientFactory(Collections.<HttpClientConfiguration>emptyList(), false);
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new GuavaModule());

        final HttpRequestHelper[] requestHelpers = new HttpRequestHelper[REMOTE_HOSTS.length];
        for (int i = 0; i < REMOTE_HOSTS.length; i++) {
            String remoteHost = REMOTE_HOSTS[i];
            requestHelpers[i] = new HttpRequestHelper(httpClientFactory.createClient(null, remoteHost, REMOTE_PORT), objectMapper);
        }

        final MiruFilter constraintsFilter = new MiruFilter(MiruFilterOperation.and,
            false,
            Arrays.asList(
                MiruFieldFilter.ofTerms(MiruFieldType.primary, "objectType", 102, 1, 18, 38, 801, 1_464_927_464, -960_826_044),
                MiruFieldFilter.ofTerms(MiruFieldType.primary, "activityType", 0, 1, 11, 65)),
            null);

        SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
        long jiveCurrentTime = new JiveEpochTimestampProvider().getTimestamp();
        final long packCurrentTime = snowflakeIdPacker.pack(jiveCurrentTime, 0, 0);
        final long packThreeDays = snowflakeIdPacker.pack(TimeUnit.DAYS.toMillis(30), 0, 0);

        ExecutorService executorService = Executors.newFixedThreadPool(8);
        int numQueries = 10_000;
        final Random rand = new Random();
        for (int i = 0; i < numQueries; i++) {
            final int index = i;
            executorService.submit(() -> {
                MiruTenantId tenantId = new MiruTenantId(tenants[index % tenants.length].getBytes(Charsets.UTF_8));
                MiruTimeRange timeRange = new MiruTimeRange(packCurrentTime - packThreeDays, packCurrentTime);
                MiruRequest<TrendingQuery> query = new MiruRequest<>("test",
                    tenantId,
                    new MiruActorId(new byte[] { 3 }),
                    MiruAuthzExpression.NOT_PROVIDED,
                    new TrendingQuery(
                        Collections.singletonList(new TrendingQueryScoreSet(
                            "test",
                            Collections.singleton(TrendingQuery.Strategy.LINEAR_REGRESSION),
                            timeRange,
                            32,
                            100)),
                        constraintsFilter,
                        "parent",
                        Collections.singletonList(Collections.singletonList(new DistinctsQuery(
                            timeRange,
                            "parent",
                            null,
                            MiruFilter.NO_FILTER,
                            Lists.transform(Arrays.asList("102", "2", "38"), MiruValue::new))))),
                    MiruSolutionLogLevel.INFO);

                @SuppressWarnings("unchecked")
                MiruResponse<TrendingAnswer> response = requestHelpers[rand.nextInt(requestHelpers.length)].executeRequest(query,
                    TrendingConstants.TRENDING_PREFIX + TrendingConstants.CUSTOM_QUERY_ENDPOINT,
                    MiruResponse.class, new Class[] { TrendingAnswer.class }, null);
                /*
                if (response.totalElapsed > 100) {
                    System.out.println("tenantId=" + tenantId);
                }
                */
                System.out.println("tenantId: " + tenantId + ", index: " + index + ", totalElapsed: " + response.totalElapsed);
                assertNotNull(response);
            });
        }

        executorService.shutdown();
        long start = System.currentTimeMillis();
        while (!executorService.isTerminated()) {
            System.out.println("Awaiting completion for " + TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start));
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    @Test(enabled = false, description = "Needs REMOTE constants")
    public void testSystemRecommended() throws Exception {

        String tenant = "999"; //brewspace
        //String tenant = "Z49"; //big tenant
        MiruTenantId tenantId = new MiruTenantId(tenant.getBytes(Charsets.UTF_8));

        HttpClientFactory httpClientFactory = new HttpClientFactoryProvider()
            .createHttpClientFactory(Collections.<HttpClientConfiguration>emptyList(), false);
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new GuavaModule());

        final HttpRequestHelper[] requestHelpers = new HttpRequestHelper[REMOTE_HOSTS.length];
        for (int i = 0; i < REMOTE_HOSTS.length; i++) {
            String remoteHost = REMOTE_HOSTS[i];
            requestHelpers[i] = new HttpRequestHelper(httpClientFactory.createClient(null, remoteHost, REMOTE_PORT), objectMapper);
        }

        ExecutorService executorService = Executors.newFixedThreadPool(16);
        int numQueries = 100_000;
        final Random rand = new Random();
        for (int i = 0; i < numQueries; i++) {
            final int index = i;
            executorService.submit(() -> {
                int userId = userIds[rand.nextInt(userIds.length)]; // 2002 + rand.nextInt(2000);

                MiruFilter constraintsFilter = new MiruFilter(MiruFilterOperation.and,
                    false,
                    Arrays.asList(
                        MiruFieldFilter.ofTerms(MiruFieldType.primary, "user", "3 " + userId)),
                    null);

                MiruFilter scorableFilter = new MiruFilter(MiruFilterOperation.and,
                    false,
                    Arrays.asList(
                        MiruFieldFilter.ofTerms(MiruFieldType.primary, "parentType", "102", "38", "2", "1464927464", "96891546", "1100"),
                        MiruFieldFilter.ofTerms(MiruFieldType.primary, "activityType", "0", "1", "65")),
                    null);

                MiruTenantId tenantId1 = new MiruTenantId(tenants[index % tenants.length].getBytes(Charsets.UTF_8));
                MiruTimeRange timeRange = lookBackFromNow(TimeUnit.DAYS.toMillis(7));
                MiruRequest<RecoQuery> request = new MiruRequest<>("test",
                    tenantId1,
                    new MiruActorId(FilerIO.intBytes(userId)),
                    MiruAuthzExpression.NOT_PROVIDED,
                    new RecoQuery(
                        timeRange,
                        new DistinctsQuery(
                            timeRange,
                            "parent",
                            null,
                            new MiruFilter(MiruFilterOperation.or,
                                false,
                                null,
                                Arrays.asList(
                                    new MiruFilter(MiruFilterOperation.and,
                                        false,
                                        Arrays.asList(
                                            MiruFieldFilter.ofTerms(MiruFieldType.primary, "user", "3 " + userId),
                                            MiruFieldFilter.ofTerms(MiruFieldType.primary, "activityType", "0", "16")),
                                        null),
                                    new MiruFilter(MiruFilterOperation.and,
                                        false,
                                        Collections.singletonList(
                                            MiruFieldFilter.ofTerms(MiruFieldType.primary, "authors", "3 " + userId)),
                                        null))),
                            null),
                        constraintsFilter,
                        "parent",
                        "user",
                        "parent",
                        scorableFilter,
                        100),
                    MiruSolutionLogLevel.INFO);

                @SuppressWarnings("unchecked")
                MiruResponse<RecoAnswer> response = requestHelpers[rand.nextInt(requestHelpers.length)].executeRequest(request,
                    RecoConstants.RECO_PREFIX + RecoConstants.CUSTOM_QUERY_ENDPOINT,
                    MiruResponse.class, new Class[] { RecoAnswer.class }, null);
                /*
                if (response.totalElapsed > 100) {
                    System.out.println("tenantId=" + tenantId);
                }
                */
                System.out.println("tenantId: " + tenantId1 + ", index: " + index + ", totalElapsed: " + response.totalElapsed);
                assertNotNull(response);
            });
        }

        executorService.shutdown();
        long start = System.currentTimeMillis();
        while (!executorService.isTerminated()) {
            System.out.println("Awaiting completion for " + TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start));
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private final SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
    private final JiveEpochTimestampProvider jiveEpochTimestampProvider = new JiveEpochTimestampProvider();

    public MiruTimeRange lookBackFromNow(long elapse) {

        //elapse = TimeUnit.DAYS.toMillis(10); // HACK

        long jiveCurrentTime = jiveEpochTimestampProvider.getTimestamp();
        long packCurrentTime = snowflakeIdPacker.pack(jiveCurrentTime, 0, 0);
        long packElapse = snowflakeIdPacker.pack(elapse, 0, 0);
        return new MiruTimeRange(packCurrentTime - packElapse, packCurrentTime);
    }
}
