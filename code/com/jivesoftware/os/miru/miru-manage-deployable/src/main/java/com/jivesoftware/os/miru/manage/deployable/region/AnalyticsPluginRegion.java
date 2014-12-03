package com.jivesoftware.os.miru.manage.deployable.region;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Charsets;
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
import java.util.concurrent.ConcurrentHashMap;
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

        final String tenant;
        final int fromHoursAgo;
        final int toHoursAgo;
        final int buckets;
        final String activityTypes;
        final String users;

        public AnalyticsPluginRegionInput(String tenant, int fromHoursAgo, int toHoursAgo, int buckets, String activityTypes, String users) {
            this.tenant = tenant;
            this.fromHoursAgo = fromHoursAgo;
            this.toHoursAgo = toHoursAgo;
            this.buckets = buckets;
            this.activityTypes = activityTypes;
            this.users = users;
        }
    }

    @Override
    public String render(Optional<AnalyticsPluginRegionInput> optionalInput) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            if (optionalInput.isPresent()) {
                AnalyticsPluginRegionInput input = optionalInput.get();
                int fromHoursAgo = input.fromHoursAgo > input.toHoursAgo ? input.fromHoursAgo : input.toHoursAgo;
                int toHoursAgo = input.fromHoursAgo > input.toHoursAgo ? input.toHoursAgo : input.fromHoursAgo;

                data.put("tenant", input.tenant);
                data.put("fromHoursAgo", String.valueOf(fromHoursAgo));
                data.put("toHoursAgo", String.valueOf(toHoursAgo));
                data.put("buckets", String.valueOf(input.buckets));
                data.put("activityTypes", input.activityTypes);
                data.put("users", input.users);

                List<String> activityTypes = Lists.newArrayList();
                for (String activityType : input.activityTypes.split(",")) {
                    String trimmed = activityType.trim();
                    if (!trimmed.isEmpty()) {
                        activityTypes.add(trimmed);
                    }
                }

                List<String> users = Lists.newArrayList();
                for (String user : input.users.split(",")) {
                    String trimmed = user.trim();
                    if (!trimmed.isEmpty()) {
                        users.add(trimmed);
                    }
                }

                SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
                long jiveCurrentTime = new JiveEpochTimestampProvider().getTimestamp();
                final long packCurrentTime = snowflakeIdPacker.pack(jiveCurrentTime, 0, 0);
                final long fromTime = packCurrentTime - snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(fromHoursAgo), 0, 0);
                final long toTime = packCurrentTime - snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(toHoursAgo), 0, 0);
                List<MiruFieldFilter> fieldFilters = Lists.newArrayList();
                fieldFilters.add(new MiruFieldFilter(MiruFieldType.primary, "locale", Collections.singletonList("en")));

                MiruFilter constraintsFilter = new MiruFilter(MiruFilterOperation.and,
                    Optional.of(fieldFilters),
                    Optional.<List<MiruFilter>>absent());

                List<RequestHelper> requestHelpers = readerRequestHelpers.get(Optional.<MiruHost>absent());
                MiruResponse<AnalyticsAnswer> response = null;
                if (!input.tenant.trim().isEmpty()) {
                    MiruTenantId tenantId = new MiruTenantId(input.tenant.trim().getBytes(Charsets.UTF_8));
                    for (RequestHelper requestHelper : requestHelpers) {
                        try {
                            ImmutableMap.Builder<String, MiruFilter> analyticsFiltersBuilder = ImmutableMap.builder();
                            for (String activityType : activityTypes) {
                                if (users.isEmpty()) {
                                    analyticsFiltersBuilder.put(
                                        activityType + "=" + Type.valueOf(Integer.parseInt(activityType)).name(),
                                        new MiruFilter(MiruFilterOperation.and,
                                            Optional.of(Collections.singletonList(
                                                new MiruFieldFilter(MiruFieldType.primary,
                                                    "activityType",
                                                    Collections.singletonList(activityType)))),
                                            Optional.<List<MiruFilter>>absent()));
                                } else {
                                    for (String user : users) {
                                        analyticsFiltersBuilder.put(
                                            activityType + "=" + Type.valueOf(Integer.parseInt(activityType)).name() + ", user=" + user,
                                            new MiruFilter(MiruFilterOperation.and,
                                                Optional.of(Arrays.asList(
                                                    new MiruFieldFilter(MiruFieldType.primary,
                                                        "activityType",
                                                        Collections.singletonList(activityType)),
                                                    new MiruFieldFilter(MiruFieldType.primary,
                                                        "user",
                                                        Collections.singletonList("3 " + user))
                                                )),
                                                Optional.<List<MiruFilter>>absent()));
                                    }
                                }
                            }
                            ImmutableMap<String, MiruFilter> analyticsFilters = analyticsFiltersBuilder.build();

                            @SuppressWarnings("unchecked")
                            MiruResponse<AnalyticsAnswer> analyticsResponse = requestHelper.executeRequest(
                                new MiruRequest<>(tenantId, MiruActorId.NOT_PROVIDED, MiruAuthzExpression.NOT_PROVIDED,
                                    new AnalyticsQuery(
                                        new MiruTimeRange(fromTime, toTime),
                                        input.buckets,
                                        constraintsFilter,
                                        analyticsFilters),
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
        int headerHeight = waveforms.size() * 16;
        int w = 1024;
        int h = 600 + headerHeight;
        BufferedImage bi = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB);

        Graphics2D g = bi.createGraphics();
        PaintWaveform pw = new PaintWaveform();

        int padLeft = 32;
        int padRight = 128;
        int padTop = 32 + headerHeight;
        int padBottom = 32;

        List<Map.Entry<String, AnalyticsAnswer.Waveform>> entries = Lists.newArrayList(waveforms.entrySet());

        int labelYOffset = 32;
        for (Map.Entry<String, AnalyticsAnswer.Waveform> entry : entries) {
            long[] waveform = entry.getValue().waveform;
            double[] hits = new double[waveform.length];
            for (int i = 0; i < hits.length; i++) {
                hits[i] = waveform[i];
            }
            String prefix = new String(entry.getKey().getBytes(), Charsets.UTF_8);
            pw.paintLabels(getHashColor(entry.getKey()), hits, prefix, 0, 0, "", g, padLeft, labelYOffset);
            labelYOffset += 16;
        }

        MinMaxDouble mmd = new MinMaxDouble();
        mmd.value(0d);
        for (int i = 0; i < entries.size(); i++) {
            Map.Entry<String, AnalyticsAnswer.Waveform> entry = entries.get(i);
            long[] waveform = entry.getValue().waveform;
            for (int j = 0; j < waveform.length; j++) {
                if (i > 0) {
                    Map.Entry<String, AnalyticsAnswer.Waveform> prevEntry = entries.get(i - 1);
                    waveform[j] += prevEntry.getValue().waveform[j];
                }
                mmd.value(waveform[j]);
            }
        }

        pw.paintGrid(g, mmd, padLeft, padTop, w - padLeft - padRight, h - padTop - padBottom);

        for (int i = entries.size() - 1; i >= 0; i--) {
            Map.Entry<String, AnalyticsAnswer.Waveform> entry = entries.get(i);
            long[] waveform = entry.getValue().waveform;
            double[] hits = new double[waveform.length];
            for (int j = 0; j < hits.length; j++) {
                hits[j] = waveform[j];
            }

            pw.paintWaveform(getHashColor(entry.getKey()), hits, mmd, true, g, padLeft, padTop, w - padLeft - padRight, h - padTop - padBottom);
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
        Random random = new Random(_instance.hashCode());
        return Color.getHSBColor(random.nextFloat(), 0.7f, 0.8f);
    }

    public static Color randomPastel(Object _instance, float min, float max) {
        Random random = new Random(_instance.hashCode());
        float r = min + ((255f - max) * (random.nextInt(255) / 255f));
        float g = min + ((255f - max) * (random.nextInt(255) / 255f));
        float b = min + ((255f - max) * (random.nextInt(255) / 255f));
        return new Color((int) r, (int) g, (int) b);
    }

    private static Color getHashSolid(Object _instance) {
        int h = Math.abs(new Random(_instance.hashCode()).nextInt());

        int b = (h % 96) + 128;
        h >>= 2;
        int g = (h % 96) + 128;
        h >>= 4;
        int r = (h % 96) + 128;
        h >>= 8;
        return new Color(r, g, b);
    }


    public static enum Type {

        viewed(0),
        created(1),
        modified(2),
        commented(3),
        replied(4),
        voted(5),
        completed(6),
        updatedStatus(7),
        bookmarked(8),
        rated(9),
        blank(10),
        liked(11),
        joined(12),
        connected(13),
        followed(14),
        unfollowed(15),
        read(16),
        shared(17),
        NOTFOUND(18),
        UNAUTHORIZED(19),
        mentioned(20),
        promoted(21),
        clicked(22),
        logged_in(23),
        logged_out(24),
        applied(25),
        removed(26),
        repost(27),
        object_exclusion_added(28),
        object_exclusion_removed(29),
        context_exclusion_added(30),
        context_exclusion_removed(31),
        user_deleted(32),
        unread(33),
        register_database(34),
        manage(35),
        unmanage(36),
        tracked(37),
        untracked(38),
        allread(39),
        inUserStream(40),
        inUserInBox(41),
        inUserActivityQueue(42),
        unliked(43),
        projectCompleted(44),
        disinterest(45),
        notification(46),
        watch(47),
        unwatch(48),
        dismiss(49),
        unconnected(50),
        reshred_complete(51),
        unjoined(52),
        trace(53),
        heartbeat(54),
        moved(55),
        repairFollowHint(56),
        search(57),
        user_search(58),
        object_untrack_added(59),
        object_untrack_removed(60),
        digest(61),
        correct_answer_set(62),
        correct_answer_removed(63),
        tagged(64),
        outcome_set(65),
        outcome_removed(66),
        object_deleted(67),
        outcomes_modified(68),
        acclaim_added(69),
        acclaim_removed(70),
        acclaim_modified(71);

        private static Map<Integer, Type> typeMap = new ConcurrentHashMap<>();

        static {
            for (Type type : values()) {
                typeMap.put(type.getID(), type);
            }
        }

        private int id;

        Type(int id) {
            this.id = id;
        }

        public int getID() {
            return id;
        }

        public static Type valueOf(int id) {
            return typeMap.get(id);
        }
    }
}
