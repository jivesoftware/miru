package com.jivesoftware.os.miru.plugin.index;

import com.google.common.primitives.Bytes;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTermId;

/**
 *
 */
public class MiruIndexUtil {

    // doc: 0^bob0
    // doc: 1^bob1
    // user: bob0|12

    // doc^user: 0^bob0
    // doc|user
    // doc~
    // user~


    public MiruTermId makeBloomComposite(MiruTermId fieldValue, String fieldName) {
        return makeComposite(fieldValue, "|", fieldName);
    }

    public MiruTermId makeFieldAggregate() {
        return makeComposite(new MiruTermId(MiruSchema.RESERVED_AGGREGATE.getBytes()), "~", MiruSchema.RESERVED_AGGREGATE);
    }

    public MiruTermId makeFieldValueAggregate(MiruTermId fieldValue, String fieldName) {
        return makeComposite(fieldValue, "^", fieldName);
    }

    private MiruTermId makeComposite(MiruTermId fieldValue, String separator, String fieldName) {
        return new MiruTermId(Bytes.concat(fieldValue.getBytes(), separator.getBytes(), fieldName.getBytes()));
    }

}
