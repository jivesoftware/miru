package com.jivesoftware.os.miru.service.index.auth;

import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.query.MiruBitmaps;
import java.util.List;

/**
 *
 */
public class MiruAuthzUtils<BM> {

    private static final BaseEncoding coder = BaseEncoding.base32().lowerCase().omitPadding();

    private final MiruBitmaps<BM> bitmaps;

    public MiruAuthzUtils(MiruBitmaps<BM> bitmaps) {
        this.bitmaps = bitmaps;
    }

    public BM getCompositeAuthz(MiruAuthzExpression authzExpression, IndexRetriever<BM> retriever) throws Exception {
        List<BM> orClauses = Lists.newArrayList();
        for (String value : authzExpression.values) {
            BM valueIndex = retriever.getIndex(value);

            if (valueIndex != null) {
                orClauses.add(valueIndex);
            }
        }
        BM got = bitmaps.create();
        if (!orClauses.isEmpty()) {
            bitmaps.or(got, orClauses);
        }
        return got;
    }

    public String encode(byte[] bytes) {
        return coder.encode(bytes);
    }

    public byte[] decode(CharSequence chars) {
        return coder.decode(chars);
    }

    public static interface IndexRetriever<BM2> {

        BM2 getIndex(String authz) throws Exception;
    }
}
