package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;

/**
 *
 * @author jonathan
 */
public interface MiruAuthzIndex<BM> {

    BM getCompositeAuthz(MiruAuthzExpression authzExpression) throws Exception;

    void index(String authz, int id) throws Exception;

    void repair(String authz, int id, boolean value) throws Exception;

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
