package com.jivesoftware.os.miru.catwalk.deployable;

import com.jivesoftware.os.amza.api.PartitionClient.KeyValueFilter;
import com.jivesoftware.os.amza.api.filer.HeapFiler;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.stream.KeyValueStream;
import java.io.IOException;

/**
 *
 */
public class CatwalkKeyValueFilter implements KeyValueFilter {

    private final float gatherMinFeatureScore;

    public CatwalkKeyValueFilter(float gatherMinFeatureScore) {
        this.gatherMinFeatureScore = gatherMinFeatureScore;
    }

    @Override
    public boolean filter(byte[] prefix,
        byte[] key,
        byte[] value,
        long timestamp,
        boolean tombstoned,
        long version,
        KeyValueStream stream) throws Exception {

        HeapFiler in = HeapFiler.fromBytes(value, value.length);
        HeapFiler out = new HeapFiler((int) in.length());
        filterRewrite(in, out);

        return stream.stream(prefix, key, out.getBytes(), timestamp, tombstoned, version);
    }

    private void filterRewrite(HeapFiler in, HeapFiler out) throws IOException {

        byte version = UIO.readByte(in, "version");
        UIO.writeByte(out, version, "version");
        if (version < 1 || version > 3) {
            throw new IllegalStateException("Unexpected version " + version);
        }

        UIO.writeByte(out, UIO.readByte(in, "partitionIsClosed"), "partitionIsClosed");

        byte[] lengthBuffer = new byte[8];

        if (version < 2) {
            int modelCountsLength = UIO.readInt(in, "modelCountsLength", lengthBuffer);
            UIO.writeInt(out, modelCountsLength, "modelCountsLength", lengthBuffer);

            for (int i = 0; i < modelCountsLength; i++) {
                UIO.writeLong(out, UIO.readLong(in, "modelCount", lengthBuffer), "modelCount", lengthBuffer);
            }
        } else {
            UIO.writeLong(out, UIO.readLong(in, "modelCount", lengthBuffer), "modelCount", lengthBuffer);
        }

        UIO.writeLong(out, UIO.readLong(in, "totalCount", lengthBuffer), "totalCount", lengthBuffer);

        int scoresLength = UIO.readInt(in, "scoresLength", lengthBuffer);
        long scoresLengthFp = out.getFilePointer();
        UIO.writeInt(out, 0, "scoresLength", lengthBuffer); // rewrite later

        int accepted = 0;
        for (int i = 0; i < scoresLength; i++) {
            byte[][] terms = new byte[UIO.readInt(in, "termsLength", lengthBuffer)][];
            for (int j = 0; j < terms.length; j++) {
                terms[j] = UIO.readByteArray(in, "term", lengthBuffer);
            }
            long[] numerators;
            long maxNumerator = 0;
            if (version < 3) {
                numerators = new long[] { UIO.readLong(in, "numerator", lengthBuffer) };
            } else {
                int numeratorCount = UIO.readByte(in, "numeratorCount");
                numerators = new long[numeratorCount];
                for (int j = 0; j < numeratorCount; j++) {
                    numerators[j] = UIO.readLong(in, "numerator", lengthBuffer);
                    maxNumerator = Math.max(numerators[j], maxNumerator);
                }
            }
            long denominator = UIO.readLong(in, "denominator", lengthBuffer);
            int numPartitions = UIO.readInt(in, "numPartitions", lengthBuffer);

            if (maxNumerator > 0) {
                float s = (float) maxNumerator / denominator;
                if (s > gatherMinFeatureScore) {
                    accepted++;

                    UIO.writeInt(out, terms.length, "termsLength", lengthBuffer);
                    for (int j = 0; j < terms.length; j++) {
                        UIO.writeByteArray(out, terms[j], "term", lengthBuffer);
                    }

                    if (version < 3) {
                        UIO.writeLong(out, numerators[0], "numerator", lengthBuffer);
                    } else {
                        UIO.writeByte(out, (byte) numerators.length, "numeratorCount");
                        for (int j = 0; j < numerators.length; j++) {
                            UIO.writeLong(out, numerators[j], "numerator", lengthBuffer);
                        }
                    }

                    UIO.writeLong(out, denominator, "denominator", lengthBuffer);
                    UIO.writeInt(out, numPartitions, "numPartitions", lengthBuffer);
                }
            }
        }

        long tailFp = out.getFilePointer();
        out.seek(scoresLengthFp);
        UIO.writeInt(out, accepted, "scoresLength", lengthBuffer);
        out.seek(tailFp);

        UIO.writeLong(out, UIO.readLong(in, "smallestTimestamp", lengthBuffer), "smallestTimestamp", lengthBuffer);
        UIO.writeLong(out, UIO.readLong(in, "largestTimestamp", lengthBuffer), "largestTimestamp", lengthBuffer);
    }

}
