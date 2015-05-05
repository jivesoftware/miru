package com.jivesoftware.os.miru.tools.deployable.analytics;

import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics;
import java.text.DecimalFormat;

public class PaintWaveform {

    static final DecimalFormat df = new DecimalFormat("0.0");

    public void paintGrid(Graphics g, int verticalLineCount, int horizontalLineCount, MinMaxDouble mmd, int _x, int _y, int _w, int _h) {
        g.setColor(Color.gray.brighter());
        float hs = _h / horizontalLineCount;

        for (int i = 0; i < 11; i++) {
            int y = _y + (int) (hs * i);
            g.drawLine(_x, y, _x + _w, y);
        }

        float ws = _w / (float) verticalLineCount;
        for (int i = 0; i < verticalLineCount; i++) {
            int x = _x + (int) (ws * i);
            g.drawLine(x, _y, x, _y + _h);
        }

        g.setFont(new Font("system", 0, 10));
        g.setColor(Color.black);

        double step = 1d / horizontalLineCount;
        for (int i = 0; i < (horizontalLineCount) + 1; i++) {
            int y = _y + (int) (hs * i);
            g.drawString(String.valueOf(Math.round(mmd.unzeroToOne(1.0 - i * step))), _x + _w + 8, y + 4);
        }

        g.setColor(Color.gray);
        g.drawRoundRect(_x, _y, _w, _h, 4, 4);

    }

    public void paintWaveform(Color color, double[] hits, MinMaxDouble mmd,
        boolean solid, Graphics g, int _x, int _y, int _w, int _h) {
        if (hits == null) {
            return;
        }
        mmd.value(0d);
        for (double d : hits) {
            mmd.value(d);
        }

        if (hits.length == 1) {
            hits = new double[] { hits[0], hits[0] };
        }
        float ws = (float) _w / (float) (hits.length - 1);

        g.setColor(color);
        for (int i = 1; i < hits.length; i++) {
            int fy = _y + _h - (int) (clamp(mmd.zeroToOne(hits[i - 1]), 0, 1) * _h);
            int ty = _y + _h - (int) (clamp(mmd.zeroToOne(hits[i - 0]), 0, 1) * _h);
            int fx = _x + (int) (ws * (i - 1));
            int tx = _x + (int) (ws * i);
            if (solid) {
                g.fillPolygon(new int[] { fx, fx, tx, tx }, new int[] { _y + _h, fy, ty, _y + _h }, 4);
            } else {
                g.drawLine(fx, fy, tx, ty);
            }
        }
    }

    public double clamp(double v, double min, double max) {
        if (v < min) {
            return min;
        }
        if (v > max) {
            return max;
        }
        return v;
    }

    public void paintLabels(Color color, double[] hits, String prefix, int xOffset, int yOffset, String suffix, Graphics g, int _x, int _y) {
        MinMaxDouble mmd = new MinMaxDouble();
        mmd.value(0d);
        for (double d : hits) {
            mmd.value(d);
        }

        g.setColor(color);
        g.fillRoundRect(_x + xOffset - 16, _y + yOffset - 12, 12, 12, 3, 3);
        g.setFont(new Font("system", 0, 10));
        g.setColor(Color.black);
        String summary = prefix + "    last=" + df.format(hits[hits.length - 1]) + suffix
            + "    min=" + df.format(mmd.min) + " " + suffix
            + "    max=" + df.format(mmd.max) + " " + suffix
            + "    mean=" + df.format(mmd.mean()) + " " + suffix;
        g.drawString(summary, _x + xOffset, _y + yOffset);

    }
}
