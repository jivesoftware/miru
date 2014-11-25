package com.jivesoftware.os.miru.manage.deployable.region;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
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
import java.util.List;
import java.util.Map;
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
    public String render(Optional<AnalyticsPluginRegionInput> input) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            if (input.isPresent()) {
                AnalyticsPluginRegionInput i = input.get();
                data.put("tenant", i.tenant.toString());
                data.put("hours", String.valueOf(i.hours));
                data.put("buckets", String.valueOf(i.buckets));

                final int numberOfHours = i.hours;
                SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
                long jiveCurrentTime = new JiveEpochTimestampProvider().getTimestamp();
                final long packCurrentTime = snowflakeIdPacker.pack(jiveCurrentTime, 0, 0);
                final long packNDays = snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(numberOfHours), 0, 0);
                MiruFilter constraintsFilter = new MiruFilter(MiruFilterOperation.and,
                    Optional.of(Arrays.asList(
                            new MiruFieldFilter("objectType", Lists.transform(
                                    Arrays.asList(102, 1, 18, 38, 801, 1_464_927_464, -960_826_044),
                                    Functions.toStringFunction())),
                            new MiruFieldFilter("activityType", Lists.transform(Arrays.asList(
                                        0, //viewed
                                        11, //liked
                                        1, //created
                                        65 //outcome_set
                                    ), Functions.toStringFunction()))
                        )),
                    Optional.<List<MiruFilter>>absent());

                RequestHelper requestHelper = readerRequestHelpers.get(Optional.<MiruHost>absent());
                @SuppressWarnings("unchecked")
                MiruResponse<AnalyticsAnswer> response = requestHelper.executeRequest(
                    new MiruRequest<>(i.tenant, MiruActorId.NOT_PROVIDED, MiruAuthzExpression.NOT_PROVIDED,
                        new AnalyticsQuery(
                            new MiruTimeRange(packCurrentTime - packNDays, packCurrentTime),
                            i.buckets,
                            constraintsFilter),
                        true),
                    AnalyticsConstants.ANALYTICS_PREFIX + AnalyticsConstants.CUSTOM_QUERY_ENDPOINT, MiruResponse.class,
                    new Class[]{AnalyticsAnswer.class},
                    null);

                AnalyticsAnswer.Waveform waveform = response.answer.waveform;
                data.put("elapse", String.valueOf(response.totalElapsed));
                //data.put("waveform", waveform == null ? "" : waveform.toString());

                data.put("waveform", "data:image/png;base64,"
                    + hitsToBase64PNGWaveform(waveform == null ? new long[i.buckets] : waveform.waveform));
                ObjectMapper mapper = new ObjectMapper();
                mapper.enable(SerializationFeature.INDENT_OUTPUT);
                data.put("summary", Joiner.on("\n").join(response.log) + "\n\n" + mapper.writeValueAsString(response.solutions));
            }
        } catch (Exception e) {
            log.error("Unable to retrieve data", e);
        }

        return renderer.render(template, data);
    }

    private String hitsToBase64PNGWaveform(long[] waveform) throws IOException {
        int w = 1024;
        int h = 200;
        BufferedImage bi = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB);

        Graphics2D g = bi.createGraphics();
        PaintWaveform pw = new PaintWaveform();

        MinMaxDouble mmd = new MinMaxDouble();
        double[] hits = new double[waveform.length];
        for (int i = 0; i < hits.length; i++) {
            hits[i] = waveform[i];
            mmd.value(hits[i]);
        }

        int xo = 32;
        int yo = 32;
        int pad = 64;
        pw.paintGrid(Color.DARK_GRAY, hits, mmd, "", 0, 0, "", true, g, xo, yo, w - pad, h - pad);
        pw.paintWaveform(Color.LIGHT_GRAY, hits, mmd, "", 0, 0, "", true, g, xo, yo, w - pad, h - pad);
        pw.paintLabels(Color.black, hits, mmd, "", 0, 0, "", true, g, xo, yo, w - pad, h - pad);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write(bi, "PNG", baos);

        return Base64.encodeBase64String(baos.toByteArray());
    }

    @Override
    public String getTitle() {
        return "Analytics";
    }

}
