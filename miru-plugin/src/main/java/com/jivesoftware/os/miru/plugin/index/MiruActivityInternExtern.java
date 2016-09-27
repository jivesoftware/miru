package com.jivesoftware.os.miru.plugin.index;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.Interner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema.CompositeFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.MiruInterner;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class MiruActivityInternExtern {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private static final int MAX_TERM_LENGTH = 4_096; //TODO add to schema

    private final MiruInterner<MiruIBA> ibaInterner;
    private final MiruInterner<MiruTenantId> tenantInterner;
    private final Interner<String> stringInterner;
    private final MiruTermComposer termComposer;

    public MiruActivityInternExtern(MiruInterner<MiruIBA> ibaInterner,
        MiruInterner<MiruTenantId> tenantInterner,
        Interner<String> stringInterner,
        MiruTermComposer termComposer) {
        this.ibaInterner = ibaInterner;
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
        final MiruSchema schema,
        StackBuffer stackBuffer) throws Exception {

        for (int i = fromOffset; i < fromOffset + length && i < activityAndIds.size(); i++) {
            MiruActivityAndId<MiruActivity> activiyAndId = activityAndIds.get(i);

            MiruActivity activity = activiyAndId.activity;
            internedActivityAndIds.set(i, new MiruActivityAndId<>(
                new MiruInternalActivity.Builder(schema,
                    tenantInterner.intern(activity.tenantId.getBytes()),
                    activity.time,
                    activity.version,
                    activity.realtimeDelivery,
                    internAuthz(activity.authz))
                    .putFieldsValues(internFields(activity.fieldsValues, schema, stackBuffer))
                    .putPropsValues(internProps(activity.propsValues, schema))
                    .build(),
                activiyAndId.id,
                activiyAndId.monoTimestamp));
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

    private MiruTermId[][] internFields(Map<String, List<String>> fields, MiruSchema schema, StackBuffer stackBuffer) throws Exception {
        MiruTermId[][] fieldsValues = new MiruTermId[schema.fieldCount()][];
        for (MiruFieldDefinition fieldDefinition : schema.getFieldDefinitions()) {
            List<String[]> fieldValues;

            MiruSchema.CompositeFieldDefinition[] compositeFieldDefinitions = schema.getCompositeFieldDefinitions(fieldDefinition.fieldId);
            if (compositeFieldDefinitions != null) {
                List<String[]> accumFieldValues = Lists.newArrayList();
                for (MiruSchema.CompositeFieldDefinition field : compositeFieldDefinitions) {
                    List<String> compositeFieldValues = fields.get(field.definition.name);

                    if (field.whitelist != null) {
                        List<String> filteredCompositeFieldValues = Lists.newArrayListWithCapacity(compositeFieldValues.size());
                        for (String compositeFieldValue : compositeFieldValues) {
                            if (field.definition.prefix != MiruFieldDefinition.Prefix.NONE) {
                                Iterator<String> iterator = Splitter.on((char) fieldDefinition.prefix.separator).split(compositeFieldValue).iterator();
                                if (iterator.hasNext() && field.whitelist.contains(iterator.next())) {
                                    filteredCompositeFieldValues.add(compositeFieldValue);
                                }
                            } else {
                                if (field.whitelist.contains(compositeFieldValue)) {
                                    filteredCompositeFieldValues.add(compositeFieldValue);
                                }
                            }
                        }
                        compositeFieldValues = filteredCompositeFieldValues;
                    }

                    if (compositeFieldValues == null || compositeFieldValues.isEmpty()) {
                        // missing terms for a composite field, we cannot construct a composite term for this activity
                        accumFieldValues.clear();
                        break;
                    } else if (accumFieldValues.isEmpty()) {
                        for (String compositeFieldValue : compositeFieldValues) {
                            accumFieldValues.add(new String[] { compositeFieldValue });
                        }
                    } else {
                        List<String[]> tmpFieldValues = Lists.newArrayList();
                        for (String[] accumFieldValue : accumFieldValues) {
                            for (String compositeFieldValue : compositeFieldValues) {
                                if (compositeFieldValue.length() <= MAX_TERM_LENGTH && compositeFieldValue.length() > 0) {
                                    String[] concat = new String[accumFieldValue.length + 1];
                                    System.arraycopy(accumFieldValue, 0, concat, 0, accumFieldValue.length);
                                    concat[concat.length - 1] = compositeFieldValue;
                                    tmpFieldValues.add(concat);
                                }
                            }
                        }
                        accumFieldValues = tmpFieldValues;
                    }
                }
                fieldValues = accumFieldValues;
            } else {
                List<String> values = fields.get(fieldDefinition.name);
                if (values != null) {
                    int i = 0;
                    while (i < values.size()) {
                        String fieldValue = values.get(i);
                        if (fieldValue.length() > MAX_TERM_LENGTH || fieldValue.length() == 0) {
                            log.warn("Ignored term {} because its length is zero or greater than {}.", fieldValue.length(), MAX_TERM_LENGTH);
                            // heavy-handed copy for removal from list, but the original list may be immutable, and this should be a rare occurrence
                            List<String> snip = Lists.newArrayListWithCapacity(values.size() - 1);
                            snip.addAll(values.subList(0, i));
                            snip.addAll(values.subList(i + 1, values.size()));
                            values = snip;
                        } else {
                            i++;
                        }
                    }
                    fieldValues = Lists.newArrayListWithCapacity(values.size());
                    for (String value : values) {
                        fieldValues.add(new String[] { value });
                    }
                } else {
                    fieldValues = null;
                }
            }

            if (fieldValues != null && !fieldValues.isEmpty()) {
                MiruTermId[] values = new MiruTermId[fieldValues.size()];
                for (int i = 0; i < values.length; i++) {
                    values[i] = termComposer.compose(schema, fieldDefinition, stackBuffer, fieldValues.get(i));
                }
                fieldsValues[fieldDefinition.fieldId] = values;
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
                values[i] = ibaInterner.intern(propValues.get(i).getBytes(Charsets.UTF_8));
            }
            propertyValues[propertyId] = values;
        }
        return propertyValues;
    }

    public String internString(String string) {
        return stringInterner.intern(string);
    }

    public MiruActivity extern(MiruInternalActivity activity, MiruSchema schema, StackBuffer stackBuffer) throws IOException {
        return new MiruActivity(activity.tenantId,
            activity.time,
            activity.version,
            activity.realtimeDelivery,
            activity.authz,
            externFields(activity.fieldsValues, schema, stackBuffer),
            externProps(activity.propsValues, schema));
    }

    private Map<String, List<String>> externFields(MiruTermId[][] fields, MiruSchema schema, StackBuffer stackBuffer) throws IOException {
        Map<String, List<String>> externFields = Maps.newHashMapWithExpectedSize(fields.length);
        for (int i = 0; i < fields.length; i++) {
            MiruTermId[] values = fields[i];
            if (values != null) {
                CompositeFieldDefinition[] compositeFieldDefinitions = schema.getCompositeFieldDefinitions(i);
                if (compositeFieldDefinitions != null) {
                    continue;
                }
                MiruFieldDefinition fieldDefinition = schema.getFieldDefinition(i);
                List<String> externValues = new ArrayList<>();
                for (MiruTermId value : values) {
                    externValues.add(termComposer.decompose(schema, fieldDefinition, stackBuffer, value)[0]);
                }
                externFields.put(fieldDefinition.name, externValues);
            }
        }
        return externFields;
    }

    private Map<String, List<String>> externProps(MiruIBA[][] properties, MiruSchema schema) {
        Map<String, List<String>> externProperties = Maps.newHashMapWithExpectedSize(properties.length);
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
