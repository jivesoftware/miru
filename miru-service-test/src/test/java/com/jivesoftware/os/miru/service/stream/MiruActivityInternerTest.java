package com.jivesoftware.os.miru.service.stream;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruPropertyDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.MiruInterner;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class MiruActivityInternerTest {

    MiruInterner<MiruTermId> termInterner = new MiruInterner<MiruTermId>(true) {
        @Override
        public MiruTermId create(byte[] bytes) {
            return new MiruTermId(bytes);
        }
    };

    private MiruActivityInternExtern interner;
    private MiruTenantId tenantId;
    private MiruSchema schema;
    private final Comparator<Object> hashComparator = (o1, o2) -> Integer.compare(o1.hashCode(), o2.hashCode());

    @BeforeMethod
    public void setUp() throws Exception {

        MiruInterner<MiruIBA> ibaInterner = new MiruInterner<MiruIBA>(true) {
            @Override
            public MiruIBA create(byte[] bytes) {
                return new MiruIBA(bytes);
            }
        };

        MiruInterner<MiruTenantId> tenantInterner = new MiruInterner<MiruTenantId>(true) {
            @Override
            public MiruTenantId create(byte[] bytes) {
                return new MiruTenantId(bytes);
            }
        };
        Interner<String> stringInterner = Interners.<String>newWeakInterner();
        MiruTermComposer termComposer = new MiruTermComposer(Charsets.UTF_8, this.termInterner);

        schema = new MiruSchema.Builder("test", 1)
            .setFieldDefinitions(new MiruFieldDefinition[] {
                new MiruFieldDefinition(0, "f", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE)
            })
            .setPropertyDefinitions(new MiruPropertyDefinition[] {
                new MiruPropertyDefinition(0, "p")
            })
            .build();
        interner = new MiruActivityInternExtern(ibaInterner, tenantInterner, stringInterner, termComposer);
        tenantId = new MiruTenantId("testIntern".getBytes());
    }

    @Test
    public void testIntern_complete() throws Exception {

        StackBuffer stackBuffer = new StackBuffer();

        List<MiruActivityAndId<MiruInternalActivity>> internalActivity1 = Arrays.<MiruActivityAndId<MiruInternalActivity>>asList(new MiruActivityAndId[1]);
        interner.intern(Arrays.asList(new MiruActivityAndId<>(
            new MiruActivity.Builder(tenantId, 1, 0, false, new String[] { "a", "b", "c" })
                .putAllFieldValues("f", ImmutableList.of("t1", "t2"))
                .putAllPropValues("p", ImmutableList.of("v1", "v2"))
                .build(), 0, 1L)), 0, 1, internalActivity1, schema, stackBuffer);

        List<MiruActivityAndId<MiruInternalActivity>> internalActivity2 = Arrays.<MiruActivityAndId<MiruInternalActivity>>asList(new MiruActivityAndId[1]);
        interner.intern(Arrays.asList(new MiruActivityAndId<>(
            new MiruActivity.Builder(tenantId, 2, 0, false, new String[] { "a", "b", "c" })
                .putAllFieldValues("f", ImmutableList.of("t1", "t2"))
                .putAllPropValues("p", ImmutableList.of("v1", "v2"))
                .build(), 1, 2L)), 0, 1, internalActivity2, schema, stackBuffer);

        MiruInternalActivity activity1 = internalActivity1.get(0).activity;
        MiruInternalActivity activity2 = internalActivity2.get(0).activity;

        assertTrue(activity1.tenantId == activity2.tenantId, "Different tenantIds"); // reference equality

        assertReferenceEquals(Arrays.asList(activity1.authz), Arrays.asList(activity2.authz));

        assertEquals(activity2.fieldsValues.length, activity1.fieldsValues.length);
        for (int i = 0; i < activity1.fieldsValues.length; i++) {
            assertReferenceEquals(Arrays.asList(activity1.fieldsValues[i]), Arrays.asList(activity2.fieldsValues[i]));
        }

        assertEquals(activity2.propsValues.length, activity1.propsValues.length);
        for (int i = 0; i < activity1.propsValues.length; i++) {
            assertReferenceEquals(Arrays.asList(activity1.propsValues[i]), Arrays.asList(activity2.propsValues[i]));
        }
    }

    @Test
    public void testIntern_nullAuthz() throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        List<MiruActivityAndId<MiruInternalActivity>> activity1 = Arrays.<MiruActivityAndId<MiruInternalActivity>>asList(new MiruActivityAndId[1]);
        interner.intern(Arrays.asList(new MiruActivityAndId<>(
            new MiruActivity.Builder(tenantId, 1, 0, false, null).build(), 0, 1L)), 0, 1, activity1, schema, stackBuffer);

        assertNull(activity1.get(0).activity.authz);
    }

    private <T> void assertReferenceEquals(Collection<T> first, Collection<T> second) {
        if (first.size() != second.size()) {
            Assert.fail("Collections of unequal size");
        }

        List<T> values1 = Lists.newArrayList(first);
        List<T> values2 = Lists.newArrayList(second);

        Collections.sort(values1, hashComparator);
        Collections.sort(values2, hashComparator);

        for (int i = 0; i < values1.size(); i++) {
            assertTrue(values1.get(i) == values2.get(i), "Different values " + values1.get(i) + " -> " + values2.get(i));
        }
    }

}
