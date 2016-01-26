/*
 * Copyright 2014 Jive Software Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.miru.service.index;

import com.jivesoftware.os.filer.io.ByteArrayFiler;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition.Prefix;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition.Type;
import com.jivesoftware.os.miru.api.activity.schema.MiruPropertyDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema.Builder;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.MiruInterner;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jonathan
 */
public class MiruInternalActivityMarshallerNGTest {

    MiruInterner<MiruTermId> termInterner = new MiruInterner<MiruTermId>(true) {
        @Override
        public MiruTermId create(byte[] bytes) {
            return new MiruTermId(bytes);
        }
    };

    MiruSchema schema = new Builder("test", 1)
        .setFieldDefinitions(new MiruFieldDefinition[] {
            new MiruFieldDefinition(0, "field0", Type.singleTerm, Prefix.NONE),
            new MiruFieldDefinition(1, "field1", Type.singleTerm, Prefix.NONE),
            new MiruFieldDefinition(2, "field2", Type.singleTerm, Prefix.NONE),
            new MiruFieldDefinition(3, "field3", Type.singleTerm, Prefix.NONE)
        })
        .setPropertyDefinitions(new MiruPropertyDefinition[] {
            new MiruPropertyDefinition(0, "prop0"),
            new MiruPropertyDefinition(1, "prop1"),
            new MiruPropertyDefinition(2, "prop2"),
            new MiruPropertyDefinition(3, "prop3")
        })
        .build();

    /**
     * Test of fieldValueFromFiler method, of class MiruInternalActivityMarshaller.
     */
    @Test
    public void testFieldValueFromFiler() throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruInternalActivityMarshaller instance = new MiruInternalActivityMarshaller(termInterner);
        MiruInternalActivity expResult = activity();
        ByteArrayFiler filer = new ByteArrayFiler(instance.toBytes(schema, expResult, stackBuffer));
        filer.seek(0);

        int fieldId = 3;
        MiruTermId[] fieldValueFromFiler = instance.fieldValueFromFiler(filer, fieldId, stackBuffer);
        MiruTermId[] expected = new MiruTermId[] { new MiruTermId("b".getBytes()), new MiruTermId("c".getBytes()) };
        Assert.assertEquals(fieldValueFromFiler, expected,
            Arrays.toString(fieldValueFromFiler) + "vs" + Arrays.toString(expected));

        filer.seek(0);
        int propId = 2;
        MiruIBA[] propValueFromFiler = instance.propValueFromFiler(filer, propId, stackBuffer);
        Assert.assertEquals(propValueFromFiler, new MiruIBA[] { new MiruIBA("j".getBytes()) });
    }

    /**
     * Test of fromFiler method, of class MiruInternalActivityMarshaller.
     */
    @Test
    public void testToBytesThenFromFiler() throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruInternalActivityMarshaller instance = new MiruInternalActivityMarshaller(termInterner);
        MiruInternalActivity expResult = activity();
        ByteArrayFiler filer = new ByteArrayFiler(instance.toBytes(schema, expResult, stackBuffer));
        MiruInternalActivity result = instance.fromFiler(new MiruTenantId("abc".getBytes()), filer, stackBuffer);
        Assert.assertEquals(result, expResult);
    }

    private MiruInternalActivity activity() {
        return new MiruInternalActivity(
            new MiruTenantId("abc".getBytes()),
            1,
            new String[] { "foo" }, 2,
            new MiruTermId[][] {
                null, null, { new MiruTermId("a".getBytes()) }, { new MiruTermId("b".getBytes()), new MiruTermId("c".getBytes()) }
            },
            new MiruIBA[][] {
                null, null, { new MiruIBA("j".getBytes()) }, { new MiruIBA("k".getBytes()), new MiruIBA("l".getBytes()) }
            });

    }

}
