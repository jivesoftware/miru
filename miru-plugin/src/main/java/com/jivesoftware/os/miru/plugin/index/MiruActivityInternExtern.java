package com.jivesoftware.os.miru.plugin.index;

import com.google.common.base.Charsets;
import com.google.common.collect.Interner;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class MiruActivityInternExtern {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private static final int MAX_TERM_LENGTH = 4_096; //TODO add to schema

    private final Interner<MiruIBA> ibaInterner;
    private final Interner<MiruTermId> termInterner;
    private final Interner<MiruTenantId> tenantInterner;
    private final Interner<String> stringInterner;
    private final MiruTermComposer termComposer;

    public MiruActivityInternExtern(Interner<MiruIBA> ibaInterner,
        Interner<MiruTermId> termInterner,
        Interner<MiruTenantId> tenantInterner,
        Interner<String> stringInterner,
        MiruTermComposer termComposer) {
        this.ibaInterner = ibaInterner;
        this.termInterner = termInterner;
        this.tenantInterner = tenantInterner;
        this.stringInterner = stringInterner;
        this.termComposer = termComposer;
    }

    /**
     * It is expected that activityAndIds.size() == internedActivityAndIds.size();
     *
     * @param activityAndIds
     * @param fromOffset
     * @param length
     * @param internedActivityAndIds
     * @param schema
     * @return
     */
    public void intern(List<MiruActivityAndId<MiruActivity>> activityAndIds,
        int fromOffset,
        int length,
        List<MiruActivityAndId<MiruInternalActivity>> internedActivityAndIds,
        final MiruSchema schema) {

        for (int i = fromOffset; i < fromOffset + length && i < activityAndIds.size(); i++) {
            MiruActivityAndId<MiruActivity> activiyAndId = activityAndIds.get(i);

            MiruActivity activity = activiyAndId.activity;
            internedActivityAndIds.set(i, new MiruActivityAndId<>(
                new MiruInternalActivity.Builder(schema,
                    tenantInterner.intern(activity.tenantId),
                    termComposer,
                    activity.time,
                    internAuthz(activity.authz),
                    activity.version)
                .putFieldsValues(internFields(activity.fieldsValues, schema))
                .putPropsValues(internProps(activity.propsValues, schema))
                .build(),
                activiyAndId.id));
        }
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

    private MiruTermId[][] internFields(Map<String, List<String>> fields, MiruSchema schema) {
        MiruTermId[][] fieldsValues = new MiruTermId[schema.fieldCount()][];
        for (String fieldName : fields.keySet()) {
            int fieldId = schema.getFieldId(fieldName);
            if (fieldId >= 0) {
                MiruFieldDefinition fieldDefinition = schema.getFieldDefinition(fieldId);
                List<String> fieldValues = fields.get(fieldName);
                for (int i = 0; i < fieldValues.size(); i++) {
                    String fieldValue = fieldValues.get(i);
                    if (fieldValue.length() > MAX_TERM_LENGTH || fieldValue.length() == 0) {
                        log.warn("Ignored term {} because it is equals to zero or greater than {}.", fieldValue.length(), MAX_TERM_LENGTH);
                        // heavy-handed copy for removal from list, but the original list may be immutable, and this should be a rare occurrence
                        List<String> snip = Lists.newArrayListWithCapacity(fieldValues.size() - 1);
                        snip.addAll(fieldValues.subList(0, i));
                        snip.addAll(fieldValues.subList(i + 1, fieldValues.size()));
                        fieldValues = snip;
                    }
                }
                MiruTermId[] values = new MiruTermId[fieldValues.size()];
                for (int i = 0; i < values.length; i++) {
                    values[i] = termInterner.intern(termComposer.compose(fieldDefinition, fieldValues.get(i)));
                }
                fieldsValues[fieldId] = values;
            }
        }
        return fieldsValues;
    }

    private MiruIBA[][] internProps(Map<String, List<String>> properties, MiruSchema schema) {
        MiruIBA[][] propertyValues = new MiruIBA[schema.propertyCount()][];
        for (String propertyName : properties.keySet()) {
            int propertyId = schema.getPropertyId(propertyName);
            List<String> propValues = properties.get(propertyName);
            MiruIBA[] values = new MiruIBA[propValues.size()];
            for (int i = 0; i < values.length; i++) {
                values[i] = ibaInterner.intern(new MiruIBA(propValues.get(i).getBytes(Charsets.UTF_8)));
            }
            propertyValues[propertyId] = values;
        }
        return propertyValues;
    }

    public MiruTermId internTermId(MiruTermId termId) {
        return termInterner.intern(termId);
    }

    public String internString(String string) {
        return stringInterner.intern(string);
    }

    public MiruActivity extern(MiruInternalActivity activity, MiruSchema schema) {
        return new MiruActivity(activity.tenantId,
            activity.time,
            activity.authz,
            activity.version,
            externFields(activity.fieldsValues, schema),
            externProps(activity.propsValues, schema));
    }

    private Map<String, List<String>> externFields(MiruTermId[][] fields, MiruSchema schema) {
        Map<String, List<String>> externFields = new HashMap<>();
        for (int i = 0; i < fields.length; i++) {
            MiruTermId[] values = fields[i];
            if (values != null) {
                MiruFieldDefinition fieldDefinition = schema.getFieldDefinition(i);
                List<String> externValues = new ArrayList<>();
                for (MiruTermId value : values) {
                    externValues.add(termComposer.decompose(fieldDefinition, value));
                }
                externFields.put(fieldDefinition.name, externValues);
            }
        }
        return externFields;
    }

    private Map<String, List<String>> externProps(MiruIBA[][] properties, MiruSchema schema) {
        Map<String, List<String>> externProperties = new HashMap<>();
        for (int i = 0; i < properties.length; i++) {
            MiruIBA[] values = properties[i];
            if (values != null) {
                List<String> externValues = new ArrayList<>();
                for (MiruIBA value : values) {
                    externValues.add(new String(value.getBytes(), Charsets.UTF_8));
                }
                externProperties.put(schema.getPropertyDefinition(i).name, externValues);
            }
        }
        return externProperties;
    }
}
