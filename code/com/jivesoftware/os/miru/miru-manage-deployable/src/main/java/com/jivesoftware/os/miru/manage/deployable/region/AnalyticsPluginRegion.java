package com.jivesoftware.os.miru.manage.deployable.region;

import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
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
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.manage.deployable.MiruSoyRenderer;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 *
 */
// soy.miru.page.analyticsPluginRegion
public class AnalyticsPluginRegion implements MiruPageRegion<Optional<MiruTenantId>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final RequestHelper[] requestHelpers;

    public AnalyticsPluginRegion(String template,
        MiruSoyRenderer renderer,
        RequestHelper[] requestHelpers) {
        Preconditions.checkArgument(requestHelpers.length > 0, "No request helpers provided");
        this.template = template;
        this.renderer = renderer;
        this.requestHelpers = requestHelpers;
    }

    @Override
    public String render(Optional<MiruTenantId> input) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            if (input.isPresent()) {
                final int numberOfDays = 30; //TODO input?
                SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
                long jiveCurrentTime = new JiveEpochTimestampProvider().getTimestamp();
                final long packCurrentTime = snowflakeIdPacker.pack(jiveCurrentTime, 0, 0);
                final long packNDays = snowflakeIdPacker.pack(TimeUnit.DAYS.toMillis(numberOfDays), 0, 0);
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

                RequestHelper requestHelper = requestHelpers[new Random().nextInt(requestHelpers.length)];
                @SuppressWarnings("unchecked")
                MiruResponse<AnalyticsAnswer> response = requestHelper.executeRequest(
                    new MiruRequest<>(input.get(), MiruActorId.NOT_PROVIDED, MiruAuthzExpression.NOT_PROVIDED,
                        new AnalyticsQuery(
                            new MiruTimeRange(packCurrentTime - packNDays, packCurrentTime),
                            32,
                            constraintsFilter),
                        true),
                    AnalyticsConstants.ANALYTICS_PREFIX + AnalyticsConstants.CUSTOM_QUERY_ENDPOINT, MiruResponse.class,
                    new Class[] { AnalyticsAnswer.class },
                    null);

                AnalyticsAnswer.Waveform waveform = response.answer.waveform;
                data.put("waveform", waveform.toString());
            }
        } catch (Exception e) {
            log.error("Unable to retrieve data", e);
        }

        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Hosts";
    }
}
