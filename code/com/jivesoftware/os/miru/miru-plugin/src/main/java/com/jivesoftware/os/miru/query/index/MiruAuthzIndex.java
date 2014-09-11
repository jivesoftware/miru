package com.jivesoftware.os.miru.query.index;

import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;

/**
 *
 * @author jonathan
 */
public interface MiruAuthzIndex<BM> {

    BM getCompositeAuthz(MiruAuthzExpression authzExpression) throws Exception;

    void index(String authz, int id) throws Exception;

    void repair(String authz, int id, boolean value) throws Exception;

    long sizeInMemory() throws Exception;

    long sizeOnDisk() throws Exception;

    void close();
}
