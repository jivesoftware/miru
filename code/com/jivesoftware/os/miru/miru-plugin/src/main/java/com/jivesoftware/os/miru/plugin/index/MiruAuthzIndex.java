package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;

/**
 *
 * @author jonathan
 */
public interface MiruAuthzIndex<BM> {

    BM getCompositeAuthz(MiruAuthzExpression authzExpression) throws Exception;

    void append(String authz, int id) throws Exception;

    void set(String authz, int id) throws Exception;

    void remove(String authz, int id) throws Exception;

    void close();

    public static class AuthzAndInvertedIndex<BM> {
        public final String authz;
        public final MiruInvertedIndex<BM> invertedIndex;

        public AuthzAndInvertedIndex(String authz, MiruInvertedIndex<BM> invertedIndex) {
            this.authz = authz;
            this.invertedIndex = invertedIndex;
        }
    }
}
