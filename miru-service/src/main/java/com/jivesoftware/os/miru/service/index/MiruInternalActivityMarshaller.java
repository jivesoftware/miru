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

    public MiruTermId[] fieldValueFromFiler(Filer filer, int fieldId, byte[] primitiveBuffer) throws IOException {
        filer.seek((4 + 4) + (fieldId * 4));
        filer.seek(FilerIO.readInt(filer, "offset", primitiveBuffer));
        return valuesFromFiler(filer, primitiveBuffer);
    }

    public MiruIBA[] propValueFromFiler(Filer filer, int propId, byte[] primitiveBuffer) throws IOException {
        int fieldsLength = FilerIO.readInt(filer, "fieldsLength", primitiveBuffer);
        filer.seek((4 + 4) + (fieldsLength * 4) + (propId * 4));
        filer.seek(FilerIO.readInt(filer, "offset", primitiveBuffer));
        return propsFromFiler(filer, primitiveBuffer);
    }

    public MiruInternalActivity fromFiler(MiruTenantId tenant, Filer filer, byte[] primitiveBuffer) throws IOException {
        int fieldsLength = FilerIO.readInt(filer, "fieldsLength", primitiveBuffer);
        int propsLength = FilerIO.readInt(filer, "propsLength", primitiveBuffer);
        MiruTermId[][] values = new MiruTermId[fieldsLength][];
        MiruIBA[][] props = new MiruIBA[propsLength][];
        for (int i = 0; i < fieldsLength + propsLength; i++) {
            FilerIO.readInt(filer, "offest", primitiveBuffer);
        }
        for (int i = 0; i < fieldsLength; i++) {
            values[i] = valuesFromFiler(filer, primitiveBuffer);
        }
        for (int i = 0; i < propsLength; i++) {
            props[i] = propsFromFiler(filer, primitiveBuffer);
        }

        long time = FilerIO.readLong(filer, "time", primitiveBuffer);
        long version = FilerIO.readLong(filer, "version", primitiveBuffer);
        String[] authz = FilerIO.readStringArray(filer, "authz", primitiveBuffer);

        return new MiruInternalActivity(tenant, time, authz, version, values, props);
    }

    private MiruTermId[] valuesFromFiler(Filer filer, byte[] primitiveBuffer) throws IOException {
        int length = FilerIO.readInt(filer, "length", primitiveBuffer);
        if (length <= 0) {
            return null;
        }
        MiruTermId[] terms = new MiruTermId[length];
        for (int i = 0; i < length; i++) {
            int l = FilerIO.readInt(filer, "length", primitiveBuffer);
            byte[] bytes = new byte[l];
            FilerIO.read(filer, bytes);
            terms[i] = new MiruTermId(bytes);
        }

        return terms;
    }

    private MiruIBA[] propsFromFiler(Filer filer, byte[] primitiveBuffer) throws IOException {
        int length = FilerIO.readInt(filer, "length", primitiveBuffer);
        if (length <= 0) {
            return null;
        }
        MiruIBA[] terms = new MiruIBA[length];
        for (int i = 0; i < length; i++) {
            int l = FilerIO.readInt(filer, "length", primitiveBuffer);
            byte[] bytes = new byte[l];
            FilerIO.read(filer, bytes);
            terms[i] = new MiruIBA(bytes);
        }

        return terms;
    }

    public byte[] toBytes(MiruInternalActivity activity, byte[] primitiveBuffer) throws IOException {

        int fieldsLength = activity.fieldsValues.length;
        int propsLength = activity.propsValues.length;

        byte[][] valueBytes = new byte[fieldsLength + propsLength][];
        int[] offsetIndex = new int[fieldsLength + propsLength];
        // fieldsLength + propsLength + fieldsIndex * 4 + propsIndex * 4
        int offset = (4 + 4) + (fieldsLength * 4) + (propsLength * 4);
        for (int i = 0; i < fieldsLength; i++) {
            offsetIndex[i] = offset;
            valueBytes[i] = fieldValuesToBytes(activity.fieldsValues[i], primitiveBuffer);
            offset += valueBytes[i].length;
        }

        for (int i = 0; i < propsLength; i++) {
            int ii = i + fieldsLength;
            offsetIndex[ii] = offset;
            valueBytes[ii] = fieldValuesToBytes(activity.propsValues[i], primitiveBuffer);
            offset += valueBytes[ii].length;
        }

        ByteArrayFiler filer = new ByteArrayFiler();
        FilerIO.writeInt(filer, fieldsLength, "fieldsLength", primitiveBuffer);
        FilerIO.writeInt(filer, propsLength, "propsLength", primitiveBuffer);
        for (int index : offsetIndex) {
            FilerIO.writeInt(filer, index, "index", primitiveBuffer);
        }
        for (byte[] value : valueBytes) {
            FilerIO.write(filer, value);
        }

        FilerIO.writeLong(filer, activity.time, "time", primitiveBuffer);
        FilerIO.writeLong(filer, activity.version, "version", primitiveBuffer);
        FilerIO.writeStringArray(filer, activity.authz, "authz", primitiveBuffer);
        return filer.getBytes();
    }

    private byte[] fieldValuesToBytes(MiruIBA[] values, byte[] primitiveBuffer) throws IOException {
        ByteArrayFiler filer = new ByteArrayFiler();
        if (values == null) {
            FilerIO.writeInt(filer, -1, "length", primitiveBuffer);
        } else {
            FilerIO.writeInt(filer, values.length, "length", primitiveBuffer);
            for (MiruIBA v : values) {
                byte[] bytes = v.immutableBytes();
                FilerIO.writeInt(filer, bytes.length, "length", primitiveBuffer);
                FilerIO.write(filer, bytes);
            }
        }
        return filer.getBytes();
    }

}
