package com.jivesoftware.os.miru.sea.anomaly.deployable.region;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.sea.anomaly.deployable.endpoints.MinMaxDouble;
import com.jivesoftware.os.miru.sea.anomaly.deployable.endpoints.PaintWaveform;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.imageio.ImageIO;
import org.apache.commons.net.util.Base64;

/**
 * @author jonathan.colt
 */
public class PNGWaveforms {

    public String hitsToBase64PNGWaveform(int width,
        int height,
        int padding,
        int horizontalLineCount,
        Map<String, long[]> waveforms,
        Optional<MinMaxDouble> bounds) {

        int headerHeight = waveforms.size() * 16;
        int w = width;
        int h = height + headerHeight;
        BufferedImage bi = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB);

        Graphics2D g = bi.createGraphics();
        g.setRenderingHint(
            RenderingHints.KEY_TEXT_ANTIALIASING,
            RenderingHints.VALUE_TEXT_ANTIALIAS_ON
        );
        g.setRenderingHint(
            RenderingHints.KEY_ANTIALIASING,
            RenderingHints.VALUE_ANTIALIAS_ON
        );
        PaintWaveform pw = new PaintWaveform();

        int padLeft = padding;
        int padRight = 128;
        int padTop = padding + headerHeight;
        int padBottom = padding;

        List<Map.Entry<String, long[]>> entries = Lists.newArrayList(waveforms.entrySet());
        Collections.sort(entries, (o1, o2) -> {
            return Long.compare(rank(o2.getValue()), rank(o1.getValue())); // reverse
        });

        int labelYOffset = padding;
        int maxWaveformLength = 0;
        for (int i = entries.size() - 1; i >= 0; i--) {
            Map.Entry<String, long[]> entry = entries.get(i);
            long[] waveform = entry.getValue();
            maxWaveformLength = Math.max(maxWaveformLength, waveform.length);
            double[] hits = new double[waveform.length];
            for (int j = 0; j < hits.length; j++) {
                hits[j] = waveform[j];
            }
            String prefix = new String(entry.getKey().getBytes(), Charsets.UTF_8);
            pw.paintLabels(getHashColor(entry.getKey()), hits, prefix, 0, 0, "", g, padLeft, labelYOffset);
            labelYOffset += 16;
        }

        MinMaxDouble mmd;
        if (bounds.isPresent()) {
            mmd = bounds.get();
        } else {
            mmd = new MinMaxDouble();
            mmd.value(0d);

            for (int i = 0; i < entries.size(); i++) {
                Map.Entry<String, long[]> entry = entries.get(i);
                long[] waveform = entry.getValue();

                for (int j = 0; j < waveform.length; j++) {
                    if (i > 0) {
                        Map.Entry<String, long[]> prevEntry = entries.get(i - 1);
                        waveform[j] += prevEntry.getValue()[j];
                    }
                    mmd.value(waveform[j]);
                }
            }
        }
        int paintedWaveformWidth = w - padLeft - padRight;
        if (maxWaveformLength >= paintedWaveformWidth / 2) {
            maxWaveformLength = paintedWaveformWidth / 2;
        }

        pw.paintGrid(g, maxWaveformLength, horizontalLineCount, mmd, padLeft, padTop, w - padLeft - padRight, h - padTop - padBottom);

        for (int i = entries.size() - 1; i >= 0; i--) {
            Map.Entry<String, long[]> entry = entries.get(i);
            long[] waveform = entry.getValue();
            double[] hits = new double[waveform.length];
            for (int j = 0; j < hits.length; j++) {
                hits[j] = waveform[j];
            }

            pw.paintWaveform(getHashColor(entry.getKey()), hits, mmd, true, g, padLeft, padTop, paintedWaveformWidth, h - padTop - padBottom);
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            ImageIO.write(bi, "PNG", baos);
        } catch (IOException x) {
            throw new RuntimeException(x);
        }

        return Base64.encodeBase64String(baos.toByteArray());
    }

    private long rank(long[] waveform) {
        long rank = 0;
        for (int i = 0; i < waveform.length; i++) {
            rank += waveform[i];
        }
        return rank;
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

}
