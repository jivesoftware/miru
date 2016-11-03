package com.jivesoftware.os.miru.bot.deployable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.mlogger.core.AtomicCounter;
import com.jivesoftware.os.miru.bot.deployable.StatedMiruValue.State;
import com.jivesoftware.os.miru.bot.deployable.MiruBotBucketSnapshot.MiruBotBucketSnapshotFields;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import static com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition.Type.singleTerm;

class MiruBotBucket {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private Map<String, Set<StatedMiruValue>> statedMiruValues = Maps.newConcurrentMap();

    private MiruTenantId miruTenantId;
    private MiruSchema miruSchema;

    private final Random RAND = new Random();
    private AtomicCounter totalActivitiesGenerated = new AtomicCounter();

    private final int numberOfFields;
    private final int valueSizeFactor;
    private final int birthRateFactor;

    MiruBotBucket(int numberOfFields,
                  int valueSizeFactor,
                  int birthRateFactor) {
        this.numberOfFields = numberOfFields;
        this.valueSizeFactor = valueSizeFactor;
        this.birthRateFactor = birthRateFactor;
    }

    MiruSchema genSchema(MiruTenantId miruTenantId) {
        this.miruTenantId = miruTenantId;
        LOG.info("Generate a miru schema {} for {} fields.", this.miruTenantId, numberOfFields);

        MiruFieldDefinition[] miruFieldDefinitions =
                new MiruFieldDefinition[numberOfFields];

        for (int i = 0; i < numberOfFields; i++) {
            String name = "field" + i;
            miruFieldDefinitions[i] = new MiruFieldDefinition(
                    i,
                    name,
                    singleTerm,
                    MiruFieldDefinition.Prefix.NONE);
            LOG.debug("Generated field definition: {}", name);
        }

        String schemaName = "mirubot-" + numberOfFields + "-singleTerm";
        LOG.info("Generated schema: {}", schemaName);
        miruSchema = new MiruSchema.Builder(schemaName, 0)
                .setFieldDefinitions(miruFieldDefinitions)
                .build();

        return miruSchema;
    }

    List<Map<String, StatedMiruValue>> seed(int value) {
        LOG.info("Seed {} miru activities.", value);

        for (MiruFieldDefinition miruFieldDefinition : miruSchema.getFieldDefinitions()) {
            for (int i = 0; i < value; i++) {
                birthNewFieldValue(miruFieldDefinition);
            }
        }

        List<Map<String, StatedMiruValue>> res = Lists.newArrayList();
        for (int i = 0; i < value; i++) {
            Map<String, StatedMiruValue> values = Maps.newHashMap();

            for (MiruFieldDefinition miruFieldDefinition : miruSchema.getFieldDefinitions()) {
                int idx = Math.min(i, statedMiruValues.get(miruFieldDefinition.name).size() - 1);

                StatedMiruValue statedMiruValue =
                        (StatedMiruValue) statedMiruValues.get(miruFieldDefinition.name).toArray()[idx];

                values.put(miruFieldDefinition.name, statedMiruValue);
            }

            res.add(values);
        }

        return res;
    }

    StatedMiruValue birthNewFieldValue() {
        if (miruSchema.getFieldDefinitions() == null) return null;

        return birthNewFieldValue(miruSchema.getFieldDefinition(RAND.nextInt(miruSchema.getFieldDefinitions().length)));
    }

    StatedMiruValue birthNewFieldValue(MiruFieldDefinition miruFieldDefinition) {
        StatedMiruValue statedMiruValue = StatedMiruValue.birth(valueSizeFactor);
        LOG.info("Birthed field: {}:{}", miruFieldDefinition.name, statedMiruValue.value.last());

        Set<StatedMiruValue> values = statedMiruValues.computeIfAbsent(
                miruFieldDefinition.name, (k) -> Sets.newHashSet());
        values.add(statedMiruValue);

        return statedMiruValue;
    }

    void addFieldValue(MiruFieldDefinition miruFieldDefinition, MiruValue miruValue, State state) {
        LOG.info("Add pre-generated field: {}:{}", miruFieldDefinition.name, miruValue.last());

        Set<StatedMiruValue> values = statedMiruValues.computeIfAbsent(
                miruFieldDefinition.name, (key) -> Sets.newHashSet());
        values.add(new StatedMiruValue(miruValue, state));
    }

    Map<String, StatedMiruValue> genWriteMiruActivity() throws Exception {
        if (miruSchema == null) {
            throw new Exception("Schema must be generated before generating a miru activity.");
        }

        totalActivitiesGenerated.inc();
        if (totalActivitiesGenerated.getCount() % birthRateFactor == 0) {
            birthNewFieldValue();
        }
        LOG.debug("Total generated activities: {}", totalActivitiesGenerated.getCount());

        Map<String, StatedMiruValue> res = Maps.newHashMap();
        for (MiruFieldDefinition miruFieldDefinition : miruSchema.getFieldDefinitions()) {
            Set<StatedMiruValue> values = statedMiruValues.computeIfAbsent(
                    miruFieldDefinition.name, (key) -> Sets.newHashSet());

            LOG.debug("Create list of non-failed reads");
            Iterator<StatedMiruValue> iter =
                    values.stream().filter(smv -> smv.state != State.READ_FAIL).iterator();
            List<StatedMiruValue> statedMiruValueList = Lists.newArrayList();
            iter.forEachRemaining(statedMiruValueList::add);

            LOG.debug("Select random value, or birth as necessary");
            StatedMiruValue statedMiruValue;
            if (statedMiruValueList.size() == 0) {
                statedMiruValue = birthNewFieldValue(miruFieldDefinition);
            } else {
                statedMiruValue = (StatedMiruValue) statedMiruValueList.toArray()[RAND.nextInt(statedMiruValueList.size())];
            }

            res.put(miruFieldDefinition.name, statedMiruValue);
        }

        return res;
    }

    List<StatedMiruValue> getActivitiesForField(MiruFieldDefinition miruFieldDefinition) {
        List<StatedMiruValue> res = Lists.newArrayList();

        statedMiruValues.get(miruFieldDefinition.name).forEach((smv) -> {
            if (smv.state != State.UNKNOWN) {
                res.add(smv);
            }
        });

        return res;
    }

    String getFieldsValues(State state) {
        StringBuilder res = new StringBuilder();

        for (Entry<String, Set<StatedMiruValue>> values : statedMiruValues.entrySet()) {
            values.getValue().forEach(smv -> {
                if (smv.state == state) {
                    if (res.length() > 0) res.append(",");

                    res.append(values.getKey());
                    res.append(":");
                    res.append(smv.value.last());
                }
            });
        }

        return res.toString();
    }

    long getFieldsValuesCount() {
        long res = 0;

        for (Entry<String, Set<StatedMiruValue>> values : statedMiruValues.entrySet()) {
            res += values.getValue().size();
        }

        return res;
    }

    long getFieldsValuesCount(State state) {
        long res = 0;

        for (Entry<String, Set<StatedMiruValue>> values : statedMiruValues.entrySet()) {
            for (StatedMiruValue statedMiruValue : values.getValue()) {
                if (statedMiruValue.state == state) {
                    res++;
                }
            }
        }

        return res;
    }

    MiruBotBucketSnapshot genSnapshot() {
        return new MiruBotBucketSnapshot(
                miruSchema.getName(),
                miruTenantId.toString(),
                totalActivitiesGenerated.getCount(),
                getFieldsValuesCount(),
                new MiruBotBucketSnapshotFields(
                        getFieldsValuesCount(State.UNKNOWN),
                        getFieldsValuesCount(State.WRITTEN),
                        getFieldsValuesCount(State.READ_FAIL),
                        getFieldsValuesCount(State.READ_SUCCESS)),
                getFieldsValues(State.READ_FAIL));
    }

}
