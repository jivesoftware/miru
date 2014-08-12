package com.jivesoftware.os.miru.service.index;

import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.googlecode.javaewah.FastAggregation;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import java.util.List;

/**
 *
 */
public class MiruAuthzUtils {

    private static final BaseEncoding coder = BaseEncoding.base32().lowerCase().omitPadding();

    private final int bitsetBufferSize;

    public MiruAuthzUtils(int bitsetBufferSize) {
        this.bitsetBufferSize = bitsetBufferSize;
    }

    public EWAHCompressedBitmap getCompositeAuthz(MiruAuthzExpression authzExpression, IndexRetriever retriever) throws Exception {
        List<EWAHCompressedBitmap> orClauses = Lists.newArrayList();
        for (String value : authzExpression.values) {
            EWAHCompressedBitmap valueIndex = retriever.getIndex(value);

            if (valueIndex != null) {
                orClauses.add(valueIndex);
            }
        }
        EWAHCompressedBitmap got = new EWAHCompressedBitmap();
        if (!orClauses.isEmpty()) {
            FastAggregation.bufferedorWithContainer(got, bitsetBufferSize, orClauses.toArray(new EWAHCompressedBitmap[orClauses.size()]));
        }
        return got;
    }

    public String encode(byte[] bytes) {
        return coder.encode(bytes);
    }

    public byte[] decode(CharSequence chars) {
        return coder.decode(chars);
    }

    public static interface IndexRetriever {

        EWAHCompressedBitmap getIndex(String authz) throws Exception;
    }
}
