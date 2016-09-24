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
import java.util.Collections;
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

    public MiruPageRegion<Void> stats(String group, String filter) {
        return new MiruPageRegion<Void>() {
            @Override
            public String getTitle() {
                return group;
            }

            @Override
            public String render(Void input) {
                Map<String, Object> data = Maps.newHashMap();
                String f = filter;
                if (f != null && f.equals("*")) {
                    f = null;
                }

                LOG.info("Rendering {} {}", group, filter);

                try {
                    if (group.equals("global")) {
                        data.put("stats", packStats(group + "-", global, group, f));
                    }
                    if (group.equals("rebuild")) {
                        data.put("stats", packStats(group + "-", rebuild, group, f));
                    }

                } catch (Exception e) {
                    LOG.error("Failed to render partitions region", e);
                }

                return renderer.render(template, data);
            }
        };
    }

    @Override
    public String render(Void input) {
        Map<String, Object> data = Maps.newHashMap();

        try {
//            data.put("rebuild", packStats("rebuild-", rebuild, null));
//            data.put("global", packStats("global-", global, null));

        } catch (Exception e) {
            LOG.error("Failed to render partitions region", e);
        }

        return renderer.render(template, data);
    }

    private List<Map<String, Object>> packStats(String prefix, LABStats stats, String group, String filter) {

        List<Map<String, Object>> list = Lists.newArrayList();

        list.addAll(wavformGroup(group, filter, prefix + "gc", defaultColors, new String[]{"gc", "pressureCommit", "commit", "fsyncedCommit", "gcCommit"},
            new LABSparseCircularMetricBuffer[]{stats.mGC, stats.mPressureCommit, stats.mCommit, stats.mFsyncedCommit, stats.mGCCommit},
            new boolean[]{false, false, false, false, false}));

        list.addAll(wavformGroup(group, filter, prefix + "lsm", defaultColors, new String[]{"open", "closed", "merging", "merged", "splitting", "split"},
            new LABSparseCircularMetricBuffer[]{stats.mOpen, stats.mClosed, stats.mMerging, stats.mMerged, stats.mSplitings, stats.mSplits},
            new boolean[]{false, false, false, false, false, false}));

        list.addAll(wavformGroup(group, filter, prefix + "mem", defaultColors, new String[]{"released", "allocationed", "slabbed", "freed"},
            new LABSparseCircularMetricBuffer[]{stats.mReleased, stats.mAllocationed, stats.mSlabbed, stats.mFreed},
            new boolean[]{false, false, false, false}));

        list.addAll(wavformGroup(group, filter, prefix + "disk", defaultColors, new String[]{"bytesWrittenToWAL", "bytesWrittenAsIndex", "bytesWrittenAsMerge",
            "bytesWrittenAsSplit"},
            new LABSparseCircularMetricBuffer[]{stats.mBytesWrittenToWAL, stats.mBytesWrittenAsIndex, stats.mBytesWrittenAsMerge, stats.mBytesWrittenAsSplit},
            new boolean[]{false, false, false, false, false}));

        String[] labels = new String[32 + 1];
        LABSparseCircularMetricBuffer[] waveforms = new LABSparseCircularMetricBuffer[32 + 1];
        boolean[] fill = new boolean[32 + 1];

        labels[32] = "total";
        waveforms[32] = stats.mEntriesWritten;
        for (int i = 0; i < 32; i++) {
            labels[i] = "2^" + (i);
            waveforms[i] = stats.mEntriesWrittenBatchPower[i];
        }

        list.addAll(wavformGroup(group, filter, prefix + "tally", histoColors, labels, waveforms, fill));

        list.addAll(wavformGroup(group, filter, prefix + "rw", defaultColors, new String[]{"append", "journaledAppend", "gets", "rangeScan", "multiRangeScan",
            "rowScan"},
            new LABSparseCircularMetricBuffer[]{stats.mAppend, stats.mJournaledAppend, stats.mGets, stats.mRangeScan, stats.mMultiRangeScan, stats.mRowScan},
            new boolean[]{false, false, false, false, false, false}));

        return list;
    }

    private Color[] histoColors = new Color[]{
        gray(255),
        gray(250),
        gray(245),
        gray(240),
        gray(235),
        gray(230),
        gray(225),
        gray(220),
        gray(215),
        gray(210),
        gray(205),
        gray(200),
        gray(195),
        gray(190),
        gray(185),
        gray(180),
        gray(175),
        gray(170),
        gray(165),
        gray(160),
        gray(155),
        gray(150),
        gray(145),
        gray(140),
        gray(135),
        gray(130),
        gray(125),
        gray(120),
        gray(115),
        gray(110),
        gray(105),
        gray(100),
        Color.green
    };

    private Color gray(int g) {
        return new Color(g, g, g);
    }

    private Color[] defaultColors = new Color[]{
        Color.blue,
        Color.green,
        Color.red,
        Color.orange,
        new Color(215, 120, 40), // brown
        Color.gray,
        Color.pink,
        Color.cyan
    };

    private List<Map<String, Object>> wavformGroup(String group, String filter, String title, Color[] colors, String[] waveName,
        LABSparseCircularMetricBuffer[] waveforms,
        boolean[] fill) {
        if (filter != null && filter.length() > 0 && !title.contains(filter)) {
            return Collections.emptyList();
        }

        String total = "";
        List<String> ls = new ArrayList<>();
        List<Map<String, Object>> ws = new ArrayList<>();
        int s = 1;
        for (double m : waveforms[0].metric()) {
            ls.add("\"" + s + "\"");
            s++;
        }

        for (int i = 0; i < waveName.length; i++) {
            List<String> values = Lists.newArrayList();
            double[] metric = waveforms[i].metric();
            for (double m : metric) {
                values.add("\"" + String.valueOf(m) + "\"");
            }
            ws.add(waveform(waveName[i], colors[i], 1f, values, fill[i], false));
            if (i > 0) {
                total += ", ";
            }
            Color c = colors[i];
            int r = c.getRed();
            int g = c.getGreen();
            int b = c.getBlue();
            String colorDiv = "<div style=\"display:inline-block; width:10px; height:10px; background:rgb(" + r + "," + g + "," + b + ");\"></div>";

            total += colorDiv + waveName[i] + "=" + numberFormat.format(waveforms[i].total());
        }

        List<Map<String, Object>> listOfwaveformGroups = Lists.newArrayList();

        List<Map<String, Object>> ows = new ArrayList<>();
        List<String> ols = new ArrayList<>();
        List<String> ovalues = Lists.newArrayList();
        for (int i = 0; i < waveforms.length; i++) {
            ovalues.add("\"" + String.valueOf(waveforms[i].total()) + "\"");
            ols.add("\"" + waveName[i] + "\"");
        }
        ows.add(waveform(title + "-overview", Color.gray, 1f, ovalues, true, false));

        Map<String, Object> overViewMap = new HashMap<>();
        overViewMap.put("title", title + "-overview");
        overViewMap.put("total", "");
        overViewMap.put("width", String.valueOf(ls.size() * 10));
        overViewMap.put("id", title + "-overview");
        overViewMap.put("graphType", "Line");
        overViewMap.put("waveform", ImmutableMap.of("labels", ols, "datasets", ows));
        listOfwaveformGroups.add(overViewMap);

        Map<String, Object> map = new HashMap<>();
        map.put("group", group);
        map.put("filter", title);
        map.put("title", title);
        map.put("total", total);
        map.put("height", String.valueOf(1000 / waveName.length));
        map.put("width", String.valueOf(ls.size() * 10));
        map.put("id", title);
        map.put("graphType", "Line");
        map.put("waveform", ImmutableMap.of("labels", ls, "datasets", ws));
        listOfwaveformGroups.add(map);
        return listOfwaveformGroups;
    }

    public Map<String, Object> waveform(String label, Color color, float alpha, List<String> values, boolean fill, boolean stepped) {
        Map<String, Object> waveform = new HashMap<>();
        waveform.put("label", "\"" + label + "\"");
        //waveform.put("steppedLine", "true");

        String c = "\"rgba(" + color.getRed() + "," + color.getGreen() + "," + color.getBlue() + "," + String.valueOf(alpha) + ")\"";
        waveform.put("fill", fill);
        waveform.put("steppedLine", stepped);
        waveform.put("lineTension", "0.1");
        waveform.put("backgroundColor", c);
        waveform.put("borderColor", c);
        waveform.put("borderCapStyle", "'butt'");
        waveform.put("borderDash", "[]");
        waveform.put("borderDashOffset", 0.0);
        waveform.put("borderJoinStyle", "'miter'");
        waveform.put("pointBorderColor", c);
        waveform.put("pointBackgroundColor", "\"#fff\"");
        waveform.put("pointBorderWidth", 1);
        waveform.put("pointHoverRadius", 5);
        waveform.put("pointHoverBackgroundColor", c);
        waveform.put("pointHoverBorderColor", c);
        waveform.put("pointHoverBorderWidth", 2);
        waveform.put("pointRadius", 1);
        waveform.put("pointHitRadius", 10);
        waveform.put("spanGaps", false);

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
