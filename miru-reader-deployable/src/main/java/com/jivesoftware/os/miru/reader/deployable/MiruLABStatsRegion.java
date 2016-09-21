package com.jivesoftware.os.miru.reader.deployable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.lab.LABStats;
import com.jivesoftware.os.lab.guts.LABSparseCircularMetricBuffer;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.awt.Color;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            try {
                rebuild.refresh();
                global.refresh();
            } catch (Exception x) {
                LOG.warn("Refresh labstats failed", x);
            }
        }, 0, 1, TimeUnit.SECONDS);
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

    private List<Map<String, Object>> packStats(LABStats stats) {

        List<Map<String, Object>> list = Lists.newArrayList();
        list.add(wavformGroup("labs", new String[]{"open", "closed"},
            new LABSparseCircularMetricBuffer[]{stats.mOpen, stats.mClosed}));

        list.add(wavformGroup("memory", new String[]{"allocationed", "freed", "slack"},
            new LABSparseCircularMetricBuffer[]{stats.mAllocationed, stats.mFreed, stats.mSlack}));

        list.add(wavformGroup("appends", new String[]{"append", "journaledAppend"},
            new LABSparseCircularMetricBuffer[]{stats.mAppend, stats.mJournaledAppend}));

        list.add(wavformGroup("writes", new String[]{"bytesWrittenToWAL", "bytesWrittenAsIndex", "bytesWrittenAsSplit", "bytesWrittenAsMerge"},
            new LABSparseCircularMetricBuffer[]{stats.mBytesWrittenToWAL, stats.mBytesWrittenAsIndex, stats.mBytesWrittenAsSplit, stats.mBytesWrittenAsMerge}));

        list.add(wavformGroup("reads", new String[]{"gets", "rangeScan", "multiRangeScan", "rowScan"},
            new LABSparseCircularMetricBuffer[]{stats.mGets, stats.mRangeScan, stats.mMultiRangeScan, stats.mRowScan}));

        list.add(wavformGroup("commits", new String[]{"commit", "fsyncedCommit"},
            new LABSparseCircularMetricBuffer[]{stats.mCommit, stats.mFsyncedCommit}));

        list.add(wavformGroup("merge", new String[]{"merging", "merged"},
            new LABSparseCircularMetricBuffer[]{stats.mMerging, stats.mMerged}));

        list.add(wavformGroup("splt", new String[]{"splitting", "split"},
            new LABSparseCircularMetricBuffer[]{stats.mSplitings, stats.mSplits}));
        return list;
    }

    private Color[] colors = new Color[]{
        Color.green, Color.blue, Color.yellow, Color.magenta, Color.orange, Color.pink
    };

    private Map<String, Object> wavformGroup(String title, String[] labels, LABSparseCircularMetricBuffer[] waveforms) {
        String total = "";
        List<String> ls = new ArrayList<>();
        List<Map<String, Object>> ws = new ArrayList<>();
        long now = System.currentTimeMillis();
        long mostRecentTimestamp = waveforms[0].mostRecentTimestamp();
        long duration = waveforms[0].duration();
        long start = now - (mostRecentTimestamp - duration);
        for (int i = 0; i < labels.length; i++) {

            List<String> values = Lists.newArrayList();
            double[] metric = waveforms[i].metric();
            long step = duration / metric.length;
            int s = 1;
            for (double m : metric) {
                values.add("\"" + String.valueOf(m) + "\"");
                ls.add("\"" + s + "\"");//humanReadableUptime(start));
                start += step;
                s++;
            }
            ws.add(waveform(labels[i], colors[i], 0.75f, values));

            if (i > 0) {
                total += ", ";
            }
            total += labels[i] + "=" + numberFormat.format(waveforms[i].total());
        }

        Map<String, Object> map = new HashMap<>();
        map.put("title", title);
        map.put("total", total);
        //map.put("lines", lines);
        //map.put("error", error);
        map.put("width", String.valueOf(ls.size() * 32));
        map.put("id", title);
        map.put("graphType", "Line");
        map.put("waveform", ImmutableMap.of("labels", ls, "datasets", ws));
        return map;
    }

    public Map<String, Object> waveform(String label, Color color, float alpha, List<String> values) {
        Map<String, Object> waveform = new HashMap<>();
        waveform.put("label", "\"" + label + "\"");
        waveform.put("fillColor", "\"rgba(" + color.getRed() + "," + color.getGreen() + "," + color.getBlue() + "," + String.valueOf(alpha) + ")\"");
        waveform.put("strokeColor", "\"rgba(" + color.getRed() + "," + color.getGreen() + "," + color.getBlue() + ",1)\"");
        waveform.put("pointColor", "\"rgba(" + color.getRed() + "," + color.getGreen() + "," + color.getBlue() + ",1)\"");
        waveform.put("pointStrokeColor", "\"rgba(" + color.getRed() + "," + color.getGreen() + "," + color.getBlue() + ",1)\"");
        waveform.put("pointHighlightFill", "\"rgba(" + color.getRed() + "," + color.getGreen() + "," + color.getBlue() + ",1)\"");
        waveform.put("pointHighlightStroke", "\"rgba(" + color.getRed() + "," + color.getGreen() + "," + color.getBlue() + ",1)\"");
        waveform.put("data", values);
        return waveform;
    }

    public static String humanReadableUptime(long millis) {
        if (millis < 0) {
            return String.valueOf(millis);
        }

        long hours = TimeUnit.MILLISECONDS.toHours(millis);
        millis -= TimeUnit.HOURS.toMillis(hours);
        long minutes = TimeUnit.MILLISECONDS.toMinutes(millis);
        millis -= TimeUnit.MINUTES.toMillis(minutes);
        long seconds = TimeUnit.MILLISECONDS.toSeconds(millis);
        millis -= TimeUnit.SECONDS.toMillis(seconds);

        StringBuilder sb = new StringBuilder(64);
        if (hours < 10) {
            sb.append('0');
        }
        sb.append(hours);
        sb.append(":");
        if (minutes < 10) {
            sb.append('0');
        }
        sb.append(minutes);
        sb.append(":");
        if (seconds < 10) {
            sb.append('0');
        }
        sb.append(seconds);

        return (sb.toString());
    }

    @Override
    public String getTitle() {
        return "LAB Stats";
    }
}
