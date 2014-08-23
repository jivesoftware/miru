package com.jivesoftware.os.miru.service.stream;

import com.google.common.collect.Interner;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import java.util.Map;

/**
 *
 */
public class MiruActivityInterner {

    private final MiruSchema schema;
    private final Interner<MiruIBA> ibaInterner;
    private final Interner<MiruTermId> termInterner;
    private final Interner<MiruTenantId> tenantInterner;
    private final Interner<String> stringInterner;

    public MiruActivityInterner(MiruSchema schema,
            Interner<MiruIBA> ibaInterner,
            Interner<MiruTermId> termInterner,
            Interner<MiruTenantId> tenantInterner,
            Interner<String> stringInterner) {
        this.schema = schema;
        this.ibaInterner = ibaInterner;
        this.termInterner = termInterner;
        this.tenantInterner = tenantInterner;
        this.stringInterner = stringInterner;
    }

    public MiruActivity intern(MiruActivity activity) {
        return new MiruActivity.Builder(schema, tenantInterner.intern(activity.tenantId), activity.time, internAuthz(activity.authz), activity.version)
            .putFieldsValues(internFields(activity.fieldsValues))
            .putPropsValues(internProps(activity.propsValues))
            .build();
    }

    private String[] internAuthz(String[] activityAuthz) {
        if (activityAuthz == null) {
            return null;
        }
        for (int i = 0; i < activityAuthz.length; i++) {
            activityAuthz[i] = stringInterner.intern(activityAuthz[i]);
        }
        return activityAuthz;
    }

    private MiruTermId[][] internFields(MiruTermId[][] fieldsValues) {
        for (int i = 0; i < fieldsValues.length; i++) {
            if (fieldsValues[i] != null) {
                for (int j = 0; j < fieldsValues[i].length; j++) {
                    fieldsValues[i][j] = termInterner.intern(fieldsValues[i][j]);
                }
            }
        }
        return fieldsValues;
    }

    private Map<String, MiruIBA[]> internProps(Map<String, MiruIBA[]> termValuesMap) {
        Map<String, MiruIBA[]> termsValues = Maps.newHashMap();
        for (Map.Entry<String, MiruIBA[]> entry : termValuesMap.entrySet()) {
            MiruIBA[] terms = new MiruIBA[entry.getValue().length];
            for (int i = 0; i < terms.length; i++) {
                terms[i] = ibaInterner.intern(entry.getValue()[i]);
            }
            termsValues.put(stringInterner.intern(entry.getKey()), terms);
        }
        return termsValues;
    }

    public MiruTermId internTermId(MiruTermId termId) {
        return termInterner.intern(termId);
    }
}
