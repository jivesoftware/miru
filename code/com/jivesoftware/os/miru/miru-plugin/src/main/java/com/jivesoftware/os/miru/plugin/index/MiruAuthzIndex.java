package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;

/**
 * @author jonathan
 */
public interface MiruAuthzIndex<BM> {

    MiruInvertedIndex<BM> getAuthz(String authz) throws Exception;

    BM getCompositeAuthz(MiruAuthzExpression authzExpression) throws Exception;

    void append(String authz, int... ids) throws Exception;

    void set(String authz, int... ids) throws Exception;

    void remove(String authz, int id) throws Exception;

    void close();

}
