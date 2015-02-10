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
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jonathan
 */
public class MiruInternalActivityMarshallerNGTest {

    /**
     * Test of fieldValueFromFiler method, of class MiruInternalActivityMarshaller.
     */
    @Test
    public void testFieldValueFromFiler() throws Exception {
        System.out.println("fieldValueFromFiler");
        MiruInternalActivityMarshaller instance = new MiruInternalActivityMarshaller();
        MiruInternalActivity expResult = activity();
        ByteArrayFiler filer = new ByteArrayFiler(instance.toBytes(expResult));
        filer.seek(0);

        int fieldId = 3;
        MiruTermId[] fieldValueFromFiler = instance.fieldValueFromFiler(filer, fieldId);
        Assert.assertEquals(fieldValueFromFiler, new MiruTermId[] { new MiruTermId("b".getBytes()), new MiruTermId("c".getBytes()) });

        filer.seek(0);
        int propId = 2;
        MiruIBA[] propValueFromFiler = instance.propValueFromFiler(filer, propId);
        Assert.assertEquals(propValueFromFiler, new MiruIBA[] { new MiruIBA("j".getBytes()) });
    }

    /**
     * Test of fromFiler method, of class MiruInternalActivityMarshaller.
     */
    @Test
    public void testToBytesThenFromFiler() throws Exception {
        System.out.println("fromFiler");
        MiruInternalActivityMarshaller instance = new MiruInternalActivityMarshaller();
        MiruInternalActivity expResult = activity();
        ByteArrayFiler filer = new ByteArrayFiler(instance.toBytes(expResult));
        MiruInternalActivity result = instance.fromFiler(new MiruTenantId("abc".getBytes()), filer);
        Assert.assertEquals(result, expResult);
    }

    private MiruInternalActivity activity() {
        return new MiruInternalActivity(new MiruTenantId("abc".getBytes()), 1, new String[] { "foo" }, 2,
            new MiruTermId[][] {
                null, null, { new MiruTermId("a".getBytes()) }, { new MiruTermId("b".getBytes()), new MiruTermId("c".getBytes()) }
            },
            new MiruIBA[][] {
                null, null, { new MiruIBA("j".getBytes()) }, { new MiruIBA("k".getBytes()), new MiruIBA("l".getBytes()) }
            });

    }

}
