package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;

/**
 * @author jonathan
 */
public interface MiruAuthzIndex<IBM> {

    MiruInvertedIndex<IBM> getAuthz(String authz) throws Exception;

    IBM getCompositeAuthz(MiruAuthzExpression authzExpression, byte[] primitiveBuffer) throws Exception;

    void append(String authz, byte[] primitiveBuffer, int... ids) throws Exception;

    void set(String authz, byte[] primitiveBuffer, int... ids) throws Exception;

    void remove(String authz, int id, byte[] primitiveBuffer) throws Exception;

    void close();

}
