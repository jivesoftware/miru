package com.jivesoftware.os.miru.service.stream;

import com.google.common.collect.Interner;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import java.util.Map;

/**
 *
 */
public class MiruActivityInterner {

    private final Interner<MiruIBA> ibaInterner;
    private final Interner<MiruTermId> termInterner;
    private final Interner<MiruTenantId> tenantInterner;
    private final Interner<String> stringInterner;

    public MiruActivityInterner(Interner<MiruIBA> ibaInterner,
        Interner<MiruTermId> termInterner, Interner<MiruTenantId> tenantInterner, Interner<String> stringInterner) {
        this.ibaInterner = ibaInterner;
        this.termInterner = termInterner;
        this.tenantInterner = tenantInterner;
        this.stringInterner = stringInterner;
    }

    public MiruActivity intern(MiruActivity activity) {
        return new MiruActivity.Builder(tenantInterner.intern(activity.tenantId), activity.time, internAuthz(activity.authz), activity.version)
            .putFieldsValues(internFields(activity.fieldsValues))
            .putPropsValues(internProps(activity.propsValues))
            .build();
    }

    private String[] internAuthz(String[] activityAuthz) {
        if (activityAuthz == null) {
            return null;
        }
        String[] authz = new String[activityAuthz.length];
        for (int i = 0; i < authz.length; i++) {
            authz[i] = stringInterner.intern(activityAuthz[i]);
        }
        return authz;
    }

    private Map<String, MiruTermId[]> internFields(Map<String, MiruTermId[]> keyValuesMap) {
        Map<String, MiruTermId[]> fieldsValues = Maps.newHashMap();
        for (Map.Entry<String, MiruTermId[]> entry : keyValuesMap.entrySet()) {
            MiruTermId[] terms = new MiruTermId[entry.getValue().length];
            for (int i = 0; i < terms.length; i++) {
                terms[i] = termInterner.intern(entry.getValue()[i]);
            }
            fieldsValues.put(stringInterner.intern(entry.getKey()), terms);
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
}
