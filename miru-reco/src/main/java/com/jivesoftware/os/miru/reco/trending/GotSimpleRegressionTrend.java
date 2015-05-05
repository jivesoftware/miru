package com.jivesoftware.os.miru.reco.trending;

import java.nio.ByteBuffer;

/**
 * @author jonathan
 */
public class GotSimpleRegressionTrend {

    public double rank;
    public double recency;
    public double slope;
    public double slopeStdErr;
    public double intercept;
    public double interceptStdErr;
    public double rsquared;

    public GotSimpleRegressionTrend() {
    }

    @Override
    public String toString() {
        return "slope=" + slope + " intercept=" + intercept + " r^2=" + rsquared;
    }

    public void fromBytes(byte[] raw) {
        if (raw == null || raw.length == 0) {
            return;
        }
        ByteBuffer bb = ByteBuffer.wrap(raw);
        rank = bb.getDouble();
        recency = bb.getDouble();
        slope = bb.getDouble();
        slopeStdErr = bb.getDouble();
        intercept = bb.getDouble();
        interceptStdErr = bb.getDouble();
        rsquared = bb.getDouble();
    }

    public byte[] toBytes() {
        ByteBuffer bb = ByteBuffer.allocate(8 * 7);
        bb.putDouble(rank);
        bb.putDouble(recency);
        bb.putDouble(slope);
        bb.putDouble(slopeStdErr);
        bb.putDouble(intercept);
        bb.putDouble(interceptStdErr);
        bb.putDouble(rsquared);
        return bb.array();
    }
}
