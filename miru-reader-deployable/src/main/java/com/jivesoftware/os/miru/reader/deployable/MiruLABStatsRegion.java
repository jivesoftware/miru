package com.jivesoftware.os.miru.reader.deployable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.lab.LABStats;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.text.NumberFormat;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class MiruLABStatsRegion implements MiruPageRegion<Void> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final NumberFormat numberFormat = NumberFormat.getInstance();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final LABStats rebuild;
    private final LABStats global;

    public MiruLABStatsRegion(String template, MiruSoyRenderer renderer, LABStats rebuild, LABStats global) {
        this.template = template;
        this.renderer = renderer;
        this.rebuild = rebuild;
        this.global = global;
    }

    @Override
    public String render(Void input) {
        Map<String, Object> data = Maps.newHashMap();

        try {

            data.put("rebuild", packStats(rebuild));
            data.put("global", packStats(global));

        } catch (Exception e) {
            LOG.error("Failed to render partitions region", e);
        }

        return renderer.render(template, data);
    }

    private List<String> packStats(LABStats stats) {
        List<String> list = Lists.newArrayList();
        list.add("open=" + numberFormat.format(stats.open.longValue()));
        list.add("append=" + numberFormat.format(stats.append.longValue()));
        list.add("journaledAppend=" + numberFormat.format(stats.journaledAppend.longValue()));
        list.add("gets=" + numberFormat.format(stats.gets.longValue()));
        list.add("rangeScan=" + numberFormat.format(stats.rangeScan.longValue()));
        list.add("multiRangeScan=" + numberFormat.format(stats.multiRangeScan.longValue()));
        list.add("rowScan=" + numberFormat.format(stats.rowScan.longValue()));
        list.add("commit=" + numberFormat.format(stats.commit.longValue()));
        list.add("fsyncedCommit=" + numberFormat.format(stats.fsyncedCommit.longValue()));
        list.add("compacts=" + numberFormat.format(stats.compacts.longValue()));
        list.add("closed=" + numberFormat.format(stats.closed.longValue()));
        list.add("allocationed=" + numberFormat.format(stats.allocationed.longValue()) + " bytes");
        list.add("freed=" + numberFormat.format(stats.freed.longValue()) + " bytes");
        list.add("gc=" + numberFormat.format(stats.gc.longValue()));
        list.add("bytesWrittenToWAL=" + numberFormat.format(stats.bytesWrittenToWAL.longValue()) + " bytes");
        list.add("bytesWrittenAsIndex=" + numberFormat.format(stats.bytesWrittenAsIndex.longValue()) + " bytes");
        list.add("bytesWrittenAsSplit=" + numberFormat.format(stats.bytesWrittenAsSplit.longValue()) + " bytes");
        list.add("bytesWrittenAsMerge=" + numberFormat.format(stats.bytesWrittenAsMerge.longValue()) + " bytes");
        return list;
    }

    @Override
    public String getTitle() {
        return "LAB Stats";
    }
}
