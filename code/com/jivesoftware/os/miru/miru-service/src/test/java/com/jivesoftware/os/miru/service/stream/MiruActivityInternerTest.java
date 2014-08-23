package com.jivesoftware.os.miru.service.stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
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

    private MiruActivityInterner interner;
    private MiruTenantId tenantId;
    private MiruSchema schema;

    @BeforeMethod
    public void setUp() throws Exception {
        Interner<MiruIBA> ibaInterner = Interners.<MiruIBA>newWeakInterner();
        Interner<MiruTermId> termInterner = Interners.<MiruTermId>newWeakInterner();
        Interner<MiruTenantId> tenantInterner = Interners.<MiruTenantId>newWeakInterner();
        Interner<String> stringInterner = Interners.<String>newWeakInterner();

        schema = new MiruSchema(new MiruFieldDefinition(0, "f"));
        interner = new MiruActivityInterner(schema, ibaInterner, termInterner, tenantInterner, stringInterner);
        tenantId = new MiruTenantId("testIntern".getBytes());
    }

    @Test
    public void testIntern_complete() throws Exception {

        MiruActivity activity1 = interner.intern(
            new MiruActivity.Builder(schema, tenantId, 1, new String[] { "a", "b", "c" }, 0)
                .putAllFieldValues("f", ImmutableList.of("t1", "t2"))
                .putAllPropValues("p", ImmutableList.of("v1".getBytes(), "v2".getBytes()))
                .build());
        MiruActivity activity2 = interner.intern(
            new MiruActivity.Builder(schema, tenantId, 2, new String[] { "a", "b", "c" }, 0)
                .putAllFieldValues("f", ImmutableList.of("t1", "t2"))
                .putAllPropValues("p", ImmutableList.of("v1".getBytes(), "v2".getBytes()))
                .build());

        assertTrue(activity1.tenantId == activity2.tenantId, "Different tenantIds"); // reference equality

        assertReferenceEquals(Arrays.asList(activity1.authz), Arrays.asList(activity2.authz));

        assertEquals(activity2.fieldsValues.length, activity1.fieldsValues.length);
        for (int i = 0; i < activity1.fieldsValues.length; i++) {
            assertReferenceEquals(Arrays.asList(activity1.fieldsValues[i]), Arrays.asList(activity2.fieldsValues[i]));
        }

        assertReferenceEquals(activity1.propsValues.keySet(), activity2.propsValues.keySet());
        for (String key : activity1.propsValues.keySet()) {
            assertReferenceEquals(Arrays.asList(activity1.propsValues.get(key)), Arrays.asList(activity2.propsValues.get(key)));
        }
    }

    @Test
    public void testIntern_nullAuthz() throws Exception {
        MiruActivity activity1 = interner.intern(new MiruActivity.Builder(schema, tenantId, 1, null, 0).build());

        assertNull(activity1.authz);
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

    private final Comparator<Object> hashComparator = new Comparator<Object>() {
        @Override
        public int compare(Object o1, Object o2) {
            return Integer.compare(o1.hashCode(), o2.hashCode());
        }
    };
}