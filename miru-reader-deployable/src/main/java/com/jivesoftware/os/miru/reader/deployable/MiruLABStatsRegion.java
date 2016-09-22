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
            data.put("rebuild", packStats("rebuild-", rebuild));
            data.put("global", packStats("global-", global));

        } catch (Exception e) {
            LOG.error("Failed to render partitions region", e);
        }

        return renderer.render(template, data);
    }

    private List<Map<String, Object>> packStats(String prefix, LABStats stats) {

        List<Map<String, Object>> list = Lists.newArrayList();

        list.add(wavformGroup(prefix + "gc", new String[]{"gc", "pressureCommit", "commit", "fsyncedCommit", "gcCommit"},
            new LABSparseCircularMetricBuffer[]{stats.mGC, stats.mPressureCommit, stats.mCommit, stats.mFsyncedCommit, stats.mGCCommit}));

        list.add(wavformGroup(prefix + "lsm", new String[]{"open", "closed", "merging", "merged", "splitting", "split"},
            new LABSparseCircularMetricBuffer[]{stats.mOpen, stats.mClosed, stats.mMerging, stats.mMerged, stats.mSplitings,
                stats.mSplits}));

        list.add(wavformGroup(prefix + "mem", new String[]{"slabbed", "allocationed", "released", "freed"},
            new LABSparseCircularMetricBuffer[]{stats.mSlabbed, stats.mAllocationed, stats.mReleased, stats.mFreed}));

        list.add(wavformGroup(prefix + "disk", new String[]{"bytesWrittenToWAL", "bytesWrittenAsIndex",
            "bytesWrittenAsMerge", "bytesWrittenAsSplit"},
            new LABSparseCircularMetricBuffer[]{stats.mBytesWrittenToWAL, stats.mBytesWrittenAsIndex, stats.mBytesWrittenAsMerge, stats.mBytesWrittenAsSplit}));

        list.add(wavformGroup(prefix + "rw", new String[]{"append", "journaledAppend", "gets", "rangeScan", "multiRangeScan", "rowScan"},
            new LABSparseCircularMetricBuffer[]{stats.mAppend, stats.mJournaledAppend, stats.mGets, stats.mRangeScan, stats.mMultiRangeScan, stats.mRowScan}));

        return list;
    }

    private Color[] colors = new Color[]{
        Color.blue,
        Color.green,
        Color.red,
        Color.orange,
        new Color(215, 120, 40), // brown
        Color.gray,
        Color.pink,
        Color.cyan
    };

    private Map<String, Object> wavformGroup(String title, String[] labels, LABSparseCircularMetricBuffer[] waveforms) {
        String total = "";
        List<String> ls = new ArrayList<>();
        List<Map<String, Object>> ws = new ArrayList<>();
        long now = System.currentTimeMillis();
        long mostRecentTimestamp = waveforms[0].mostRecentTimestamp();
        long duration = waveforms[0].duration();
        long start = now - (mostRecentTimestamp - duration);
        int s = 1;
        for (double m : waveforms[0].metric()) {
            ls.add("\"" + s + "\"");//humanReadableUptime(start));
            s++;
        }

        for (int i = 0; i < labels.length; i++) {
            List<String> values = Lists.newArrayList();
            double[] metric = waveforms[i].metric();
            for (double m : metric) {
                values.add("\"" + String.valueOf(m) + "\"");
            }
            ws.add(waveform(labels[i], colors[i], 0.25f, values));
            if (i > 0) {
                total += ", ";
            }
            Color c = colors[i];
            int r = c.getRed();
            int g = c.getGreen();
            int b = c.getBlue();
            String colorDiv = "<div style=\"display:inline-block; width:10px; height:10px; background:rgb(" + r + "," + g + "," + b + ");\"></div>";

            total += colorDiv + labels[i] + "=" + numberFormat.format(waveforms[i].total());
        }

        Map<String, Object> map = new HashMap<>();
        map.put("title", title);
        map.put("total", total);
        //map.put("lines", lines);
        //map.put("error", error);
        map.put("width", String.valueOf(ls.size() * 10));
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
