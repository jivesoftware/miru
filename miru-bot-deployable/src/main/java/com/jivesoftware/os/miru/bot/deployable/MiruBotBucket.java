package com.jivesoftware.os.miru.bot.deployable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.mlogger.core.AtomicCounter;
import com.jivesoftware.os.miru.bot.deployable.MiruBotDistinctsInitializer.MiruBotDistinctsConfig;
import com.jivesoftware.os.miru.bot.deployable.StatedMiruValue.State;
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
    private MiruSchema miruSchema;

    private final Random RAND = new Random();
    private AtomicCounter totalActivitiesGenerated = new AtomicCounter();

    private final MiruBotDistinctsConfig miruBotDistinctsConfig;

    MiruBotBucket(MiruBotDistinctsConfig miruBotDistinctsConfig) {
        this.miruBotDistinctsConfig = miruBotDistinctsConfig;
    }

    MiruSchema genSchema() {
        LOG.info("Generate a miru schema for {} fields.", miruBotDistinctsConfig.getNumberOfFields());

        MiruFieldDefinition[] miruFieldDefinitions =
                new MiruFieldDefinition[miruBotDistinctsConfig.getNumberOfFields()];

        for (int i = 0; i < miruBotDistinctsConfig.getNumberOfFields(); i++) {
            String name = "field" + i;
            miruFieldDefinitions[i] = new MiruFieldDefinition(
                    i,
                    name,
                    singleTerm,
                    MiruFieldDefinition.Prefix.NONE);
            LOG.debug("Generated field definition: {}", name);
        }

        String name = "mirubot-" + miruBotDistinctsConfig.getNumberOfFields() + "-singleTerm";
        LOG.info("Generated schema: {}", name);
        miruSchema = new MiruSchema.Builder(name, 0)
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
        StatedMiruValue statedMiruValue = StatedMiruValue.birth(miruBotDistinctsConfig.getValueSizeFactor());
        LOG.info("Birthed field: {}:{}", miruFieldDefinition.name, statedMiruValue.value.last());

        Set<StatedMiruValue> values = statedMiruValues.computeIfAbsent(
                miruFieldDefinition.name, (key) -> Sets.newHashSet());
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
        if (totalActivitiesGenerated.getCount() % miruBotDistinctsConfig.getBirthRateFactor() == 0) {
            birthNewFieldValue();
        }
        LOG.debug("Total generated activities: {}", totalActivitiesGenerated.getCount());

        Map<String, StatedMiruValue> res = Maps.newHashMap();
        for (MiruFieldDefinition miruFieldDefinition : miruSchema.getFieldDefinitions()) {
            Set<StatedMiruValue> values = statedMiruValues.computeIfAbsent(
                    miruFieldDefinition.name, (key) -> Sets.newHashSet());

            Iterator<StatedMiruValue> iter =
                    values.stream().filter(smv -> smv.state != State.READ_FAIL).iterator();
            int count = 0;
            while (iter.hasNext()) {
                iter.next();
                count++;
            }

            StatedMiruValue statedMiruValue;
            if (count == 0) {
                statedMiruValue = birthNewFieldValue(miruFieldDefinition);
            } else {
                iter = values.stream().filter(smv -> smv.state != State.READ_FAIL).iterator();
                statedMiruValue = iter.next();
                for (int i = 1; i <= RAND.nextInt(count); i++) {
                    statedMiruValue = iter.next();
                }
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

    int getFieldsValuesCount() {
        int res = 0;

        for (Entry<String, Set<StatedMiruValue>> values : statedMiruValues.entrySet()) {
            res += values.getValue().size();
        }

        return res;
    }

    int getFieldsValuesCount(State state) {
        int res = 0;

        for (Entry<String, Set<StatedMiruValue>> values : statedMiruValues.entrySet()) {
            for (StatedMiruValue statedMiruValue : values.getValue()) {
                if (statedMiruValue.state == state) {
                    res++;
                }
            }
        }

        return res;
    }

}
