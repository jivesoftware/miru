package com.jivesoftware.os.miru.service.index.auth;

import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;

/**
 *
 * @author jonathan
 */
public interface MiruAuthzIndex {

    EWAHCompressedBitmap getCompositeAuthz(MiruAuthzExpression authzExpression) throws Exception;

    void index(String authz, int id) throws Exception;

    void repair(String authz, int id, boolean value) throws Exception;

    long sizeInMemory() throws Exception;

    long sizeOnDisk() throws Exception;

    void close();
}
