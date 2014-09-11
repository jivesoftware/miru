package com.jivesoftware.os.miru.query.index;

import com.google.common.primitives.Bytes;
import com.jivesoftware.os.miru.api.base.MiruTermId;

/**
 *
 */
public class MiruIndexUtil {

    public MiruTermId makeComposite(MiruTermId fieldValue, String separator, String fieldName) {
        return new MiruTermId(Bytes.concat(fieldValue.getBytes(), separator.getBytes(), fieldName.getBytes()));
    }

}
