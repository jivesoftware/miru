package com.jivesoftware.os.miru.manage.deployable.region;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Charsets;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsAnswer;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsConstants;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsQuery;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.manage.deployable.MiruSoyRenderer;
import com.jivesoftware.os.miru.manage.deployable.ReaderRequestHelpers;
import com.jivesoftware.os.miru.manage.deployable.analytics.MinMaxDouble;
import com.jivesoftware.os.miru.manage.deployable.analytics.PaintWaveform;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import javax.imageio.ImageIO;
import org.apache.commons.net.util.Base64;

/**
 *
 */
// soy.miru.page.analyticsPluginRegion
public class AnalyticsPluginRegion implements MiruPageRegion<Optional<AnalyticsPluginRegion.AnalyticsPluginRegionInput>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final ReaderRequestHelpers readerRequestHelpers;

    public AnalyticsPluginRegion(String template,
        MiruSoyRenderer renderer,
        ReaderRequestHelpers readerRequestHelpers) {
        this.template = template;
        this.renderer = renderer;
        this.readerRequestHelpers = readerRequestHelpers;
    }

    public static class AnalyticsPluginRegionInput {

        final MiruTenantId tenant;
        final int hours;
        final int buckets;

        public AnalyticsPluginRegionInput(MiruTenantId tenant, int hours, int buckets) {
            this.tenant = tenant;
            this.hours = hours;
            this.buckets = buckets;
        }

    }

    @Override
    public String render(Optional<AnalyticsPluginRegionInput> optionalInput) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            if (optionalInput.isPresent()) {
                AnalyticsPluginRegionInput input = optionalInput.get();
                data.put("tenant", input.tenant.toString());
                data.put("hours", String.valueOf(input.hours));
                data.put("buckets", String.valueOf(input.buckets));

                final int numberOfHours = input.hours;
                SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
                long jiveCurrentTime = new JiveEpochTimestampProvider().getTimestamp();
                final long packCurrentTime = snowflakeIdPacker.pack(jiveCurrentTime, 0, 0);
                final long packNDays = snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(numberOfHours), 0, 0);
                MiruFilter constraintsFilter = new MiruFilter(MiruFilterOperation.and,
                    Optional.of(Arrays.asList(
                        new MiruFieldFilter(MiruFieldType.primary, "objectType", Lists.transform(
                            Arrays.asList(102, 1, 18, 38, 801, 1_464_927_464, -960_826_044),
                            Functions.toStringFunction()))
                    )),
                    Optional.<List<MiruFilter>>absent());

                List<RequestHelper> requestHelpers = readerRequestHelpers.get(Optional.<MiruHost>absent());
                MiruResponse<AnalyticsAnswer> response = null;
                for (RequestHelper requestHelper : requestHelpers) {
                    try {
                        @SuppressWarnings("unchecked")
                        MiruResponse<AnalyticsAnswer> analyticsResponse = requestHelper.executeRequest(
                            new MiruRequest<>(input.tenant, MiruActorId.NOT_PROVIDED, MiruAuthzExpression.NOT_PROVIDED,
                                new AnalyticsQuery(
                                    new MiruTimeRange(packCurrentTime - packNDays, packCurrentTime),
                                    input.buckets,
                                    constraintsFilter,
                                    ImmutableMap.<String, MiruFilter>builder()
                                        .put("all",
                                            new MiruFilter(MiruFilterOperation.and,
                                                Optional.of(Collections.singletonList(
                                                    new MiruFieldFilter(MiruFieldType.primary,
                                                        "activityType",
                                                        Lists.transform(Arrays.asList(
                                                            0, //viewed
                                                            11, //liked
                                                            1, //created
                                                            65 //outcome_set
                                                        ), Functions.toStringFunction())))),
                                                Optional.<List<MiruFilter>>absent()))
                                        .put("viewed",
                                            new MiruFilter(MiruFilterOperation.and,
                                                Optional.of(Collections.singletonList(
                                                    new MiruFieldFilter(MiruFieldType.primary,
                                                        "activityType",
                                                        Lists.transform(Arrays.asList(
                                                            0 //viewed
                                                        ), Functions.toStringFunction())))),
                                                Optional.<List<MiruFilter>>absent()))
                                        .put("liked",
                                            new MiruFilter(MiruFilterOperation.and,
                                                Optional.of(Collections.singletonList(
                                                    new MiruFieldFilter(MiruFieldType.primary,
                                                        "activityType",
                                                        Lists.transform(Arrays.asList(
                                                            11 //liked
                                                        ), Functions.toStringFunction())))),
                                                Optional.<List<MiruFilter>>absent()))
                                        .put("created",
                                            new MiruFilter(MiruFilterOperation.and,
                                                Optional.of(Collections.singletonList(
                                                    new MiruFieldFilter(MiruFieldType.primary,
                                                        "activityType",
                                                        Lists.transform(Arrays.asList(
                                                            1 //created
                                                        ), Functions.toStringFunction())))),
                                                Optional.<List<MiruFilter>>absent()))
                                        .put("outcome_set",
                                            new MiruFilter(MiruFilterOperation.and,
                                                Optional.of(Collections.singletonList(
                                                    new MiruFieldFilter(MiruFieldType.primary,
                                                        "activityType",
                                                        Lists.transform(Arrays.asList(
                                                            65 //outcome_set
                                                        ), Functions.toStringFunction())))),
                                                Optional.<List<MiruFilter>>absent()))
                                        .build()),
                                true),
                            AnalyticsConstants.ANALYTICS_PREFIX + AnalyticsConstants.CUSTOM_QUERY_ENDPOINT, MiruResponse.class,
                            new Class[] { AnalyticsAnswer.class },
                            null);
                        response = analyticsResponse;
                        if (response != null && response.answer != null) {
                            break;
                        } else {
                            log.warn("Empty analytics response from {}, trying another", requestHelper);
                        }
                    } catch (Exception e) {
                        log.warn("Failed analytics request to {}, trying another", new Object[] { requestHelper }, e);
                    }
                }

                if (response != null && response.answer != null) {
                    Map<String, AnalyticsAnswer.Waveform> waveforms = response.answer.waveforms;
                    if (waveforms == null) {
                        waveforms = Collections.emptyMap();
                    }
                    data.put("elapse", String.valueOf(response.totalElapsed));
                    //data.put("waveform", waveform == null ? "" : waveform.toString());

                    data.put("waveform", "data:image/png;base64," + hitsToBase64PNGWaveform(waveforms));
                    ObjectMapper mapper = new ObjectMapper();
                    mapper.enable(SerializationFeature.INDENT_OUTPUT);
                    data.put("summary", Joiner.on("\n").join(response.log) + "\n\n" + mapper.writeValueAsString(response.solutions));
                }
            }
        } catch (Exception e) {
            log.error("Unable to retrieve data", e);
        }

        return renderer.render(template, data);
    }

    private String hitsToBase64PNGWaveform(Map<String, AnalyticsAnswer.Waveform> waveforms) throws IOException {
        int w = 1024;
        int h = 200;
        BufferedImage bi = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB);

        Graphics2D g = bi.createGraphics();
        PaintWaveform pw = new PaintWaveform();

        int xo = 32;
        int yo = 32;
        int pad = 64;
        pw.paintGrid(g, xo, yo, w - pad, h - pad);

        for (Map.Entry<String, AnalyticsAnswer.Waveform> entry : waveforms.entrySet()) {
            long[] waveform = entry.getValue().waveform;
            MinMaxDouble mmd = new MinMaxDouble();
            double[] hits = new double[waveform.length];
            for (int i = 0; i < hits.length; i++) {
                hits[i] = waveform[i];
                mmd.value(hits[i]);
            }

            pw.paintWaveform(getHashSolid(entry.getKey()), hits, mmd, false, g, xo, yo, w - pad, h - pad);
        }

        int labelYOffset = yo;
        for (Map.Entry<String, AnalyticsAnswer.Waveform> entry : waveforms.entrySet()) {
            long[] waveform = entry.getValue().waveform;
            MinMaxDouble mmd = new MinMaxDouble();
            double[] hits = new double[waveform.length];
            for (int i = 0; i < hits.length; i++) {
                hits[i] = waveform[i];
                mmd.value(hits[i]);
            }
            String prefix = new String(entry.getKey().getBytes(), Charsets.UTF_8);
            pw.paintLabels(getHashSolid(entry.getKey()), hits, mmd, prefix, 0, 0, "", g, xo, labelYOffset);
            labelYOffset += 32;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write(bi, "PNG", baos);

        return Base64.encodeBase64String(baos.toByteArray());
    }

    @Override
    public String getTitle() {
        return "Analytics";
    }

    private static Color getHashColor(Object _instance) {
        int h = new Random(_instance.hashCode()).nextInt();

        int b = (h % 222) + 32;
        h >>= 2;
        int g = (h % 222) + 32;
        h >>= 4;
        int r = (h % 222) + 32;
        h >>= 8;

        return new Color(r, g, b);
    }

    private static Color getHashSolid(Object _instance) {
        int h = new Random(_instance.hashCode()).nextInt();

        int b = (h % 96) + 128;
        h >>= 2;
        int g = (h % 96) + 128;
        h >>= 4;
        int r = (h % 96) + 128;
        h >>= 8;
        return new Color(r, g, b);
    }
}
