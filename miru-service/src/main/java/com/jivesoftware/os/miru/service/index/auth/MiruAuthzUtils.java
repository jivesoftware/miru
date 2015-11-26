package com.jivesoftware.os.miru.service.index.auth;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import java.util.List;

/**
 *
 */
public class MiruAuthzUtils<BM extends IBM, IBM> {

    private static final BaseEncoding coder = BaseEncoding.base32().lowerCase().omitPadding();
    private static final Splitter splitter = Splitter.on('.');

    private final MiruBitmaps<BM, IBM> bitmaps;

    public MiruAuthzUtils(MiruBitmaps<BM, IBM> bitmaps) {
        this.bitmaps = bitmaps;
    }

    public BM getCompositeAuthz(MiruAuthzExpression authzExpression, IndexRetriever<IBM> retriever) throws Exception {
        List<IBM> orClauses = Lists.newArrayList();
        for (String value : authzExpression.values) {
            IBM valueIndex = retriever.getIndex(value);

            if (valueIndex != null) {
                orClauses.add(valueIndex);
            }
        }
        return bitmaps.or(orClauses);
    }

    public String encode(byte[] bytes) {
        return coder.encode(bytes);
    }

    public byte[] decode(CharSequence chars) {
        return coder.decode(chars);
    }

    public static byte[] key(String authz) {
        boolean negated = authz.endsWith("#");
        if (negated) {
            authz = authz.substring(0, authz.length() - 1);
        }
        List<byte[]> bytesList = Lists.newArrayList();
        for (String authzComponent : splitter.split(authz)) {
            byte[] bytes = coder.decode(authzComponent);
            bytesList.add(bytes);
        }
        if (negated) {
            bytesList.add(new byte[] { 0 });
        }
        int length = bytesList.size() * 8;
        byte[] concatenatedAuthzBytes = new byte[length];
        int i = 0;
        for (byte[] bytes : bytesList) {
            System.arraycopy(bytes, 0, concatenatedAuthzBytes, i * 8, bytes.length);
            i++;
        }
        return concatenatedAuthzBytes;
    }

    public static interface IndexRetriever<BM2> {

        BM2 getIndex(String authz) throws Exception;
    }
}
