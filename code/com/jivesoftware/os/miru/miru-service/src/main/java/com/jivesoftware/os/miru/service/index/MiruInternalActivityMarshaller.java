/*
 * Copyright 2014 Jive Software Inc..
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
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import java.io.IOException;

/**
 *
 * @author jonathan
 */
public class MiruInternalActivityMarshaller {

    public MiruTermId[] fieldValueFromFiler(Filer filer, int fieldId) throws IOException {
        filer.seek((4 + 4) + (fieldId * 4));
        filer.seek(FilerIO.readInt(filer, "offset"));
        return valuesFromFiler(filer);
    }

    public MiruIBA[] propValueFromFiler(Filer filer, int propId) throws IOException {
        int fieldsLength = FilerIO.readInt(filer, "fieldsLength");
        filer.seek((4 + 4) + (fieldsLength * 4) + (propId * 4));
        filer.seek(FilerIO.readInt(filer, "offset"));
        return propsFromFiler(filer);
    }

    public MiruInternalActivity fromFiler(MiruTenantId tenant, Filer filer) throws IOException {
        int fieldsLength = FilerIO.readInt(filer, "fieldsLength");
        int propsLength = FilerIO.readInt(filer, "propsLength");
        MiruTermId[][] values = new MiruTermId[fieldsLength][];
        MiruIBA[][] props = new MiruIBA[propsLength][];
        for (int i = 0; i < fieldsLength + propsLength; i++) {
            FilerIO.readInt(filer, "offest");
        }
        for (int i = 0; i < fieldsLength; i++) {
            values[i] = valuesFromFiler(filer);
        }
        for (int i = 0; i < propsLength; i++) {
            props[i] = propsFromFiler(filer);
        }

        long time = FilerIO.readLong(filer, "time");
        long version = FilerIO.readLong(filer, "version");
        String[] authz = FilerIO.readStringArray(filer, "authz");

        return new MiruInternalActivity(tenant, time, authz, version, values, props);
    }

    private MiruTermId[] valuesFromFiler(Filer filer) throws IOException {
        int length = FilerIO.readInt(filer, "length");
        if (length <= 0) {
            return null;
        }
        MiruTermId[] terms = new MiruTermId[length];
        for (int i = 0; i < length; i++) {
            int l = FilerIO.readInt(filer, "length");
            byte[] bytes = new byte[l];
            FilerIO.read(filer, bytes);
            terms[i] = new MiruTermId(bytes);
        }

        return terms;
    }

    private MiruIBA[] propsFromFiler(Filer filer) throws IOException {
        int length = FilerIO.readInt(filer, "length");
        if (length <= 0) {
            return null;
        }
        MiruIBA[] terms = new MiruIBA[length];
        for (int i = 0; i < length; i++) {
            int l = FilerIO.readInt(filer, "length");
            byte[] bytes = new byte[l];
            FilerIO.read(filer, bytes);
            terms[i] = new MiruIBA(bytes);
        }

        return terms;
    }

    public byte[] toBytes(MiruInternalActivity activity) throws IOException {

        int fieldsLength = activity.fieldsValues.length;
        int propsLength = activity.propsValues.length;

        byte[][] valueBytes = new byte[fieldsLength + propsLength][];
        int[] offsetIndex = new int[fieldsLength + propsLength];
        // fieldsLength + propsLength + fieldsIndex * 4 + propsIndex * 4
        int offset = (4 + 4) + (fieldsLength * 4) + (propsLength * 4);
        for (int i = 0; i < fieldsLength; i++) {
            offsetIndex[i] = offset;
            valueBytes[i] = fieldValuesToBytes(activity.fieldsValues[i]);
            offset += valueBytes[i].length;
        }

        for (int i = 0; i < propsLength; i++) {
            int ii = i + fieldsLength;
            offsetIndex[ii] = offset;
            valueBytes[ii] = fieldValuesToBytes(activity.propsValues[i]);
            offset += valueBytes[ii].length;
        }

        ByteArrayFiler filer = new ByteArrayFiler();
        FilerIO.writeInt(filer, fieldsLength, "fieldsLength");
        FilerIO.writeInt(filer, propsLength, "propsLength");
        for (int index : offsetIndex) {
            FilerIO.writeInt(filer, index, "index");
        }
        for (byte[] value : valueBytes) {
            FilerIO.write(filer, value);
        }

        FilerIO.writeLong(filer, activity.time, "time");
        FilerIO.writeLong(filer, activity.version, "version");
        FilerIO.writeStringArray(filer, activity.authz, "authz");
        return filer.getBytes();
    }

    private byte[] fieldValuesToBytes(MiruIBA[] values) throws IOException {
        ByteArrayFiler filer = new ByteArrayFiler();
        if (values == null) {
            FilerIO.writeInt(filer, -1, "length");
        } else {
            FilerIO.writeInt(filer, values.length, "length");
            for (MiruIBA v : values) {
                byte[] bytes = v.immutableBytes();
                FilerIO.writeInt(filer, bytes.length, "length");
                FilerIO.write(filer, bytes);
            }
        }
        return filer.getBytes();
    }

}
