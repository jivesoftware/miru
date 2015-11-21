package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;

/**
 * @author jonathan
 */
public interface MiruAuthzIndex<IBM> {

    MiruInvertedIndex<IBM> getAuthz(String authz) throws Exception;

    IBM getCompositeAuthz(MiruAuthzExpression authzExpression, StackBuffer stackBuffer) throws Exception;

    void append(String authz, StackBuffer stackBuffer, int... ids) throws Exception;

    void set(String authz, StackBuffer stackBuffer, int... ids) throws Exception;

    void remove(String authz, int id, StackBuffer stackBuffer) throws Exception;

    void close();

}
