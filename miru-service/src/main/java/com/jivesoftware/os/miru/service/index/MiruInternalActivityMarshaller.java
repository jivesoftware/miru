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
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition.Feature;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.MiruInterner;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import java.io.IOException;

/**
 *
 * @author jonathan
 */
public class MiruInternalActivityMarshaller {

    private final MiruInterner<MiruTermId> termInterner;

    public MiruInternalActivityMarshaller(MiruInterner<MiruTermId> termInterner) {
        this.termInterner = termInterner;
    }

    public MiruTermId[] fieldValueFromFiler(Filer filer, int fieldId, StackBuffer stackBuffer) throws IOException {
        filer.seek((4 + 4) + (fieldId * 4));
        filer.seek(FilerIO.readInt(filer, "offset", stackBuffer));
        return valuesFromFiler(filer, stackBuffer);
    }

    public MiruIBA[] propValueFromFiler(Filer filer, int propId, StackBuffer stackBuffer) throws IOException {
        int fieldsLength = FilerIO.readInt(filer, "fieldsLength", stackBuffer);
        filer.seek((4 + 4) + (fieldsLength * 4) + (propId * 4));
        filer.seek(FilerIO.readInt(filer, "offset", stackBuffer));
        return propsFromFiler(filer, stackBuffer);
    }

    public MiruInternalActivity fromFiler(MiruTenantId tenant, Filer filer, StackBuffer stackBuffer) throws IOException {
        int fieldsLength = FilerIO.readInt(filer, "fieldsLength", stackBuffer);
        int propsLength = FilerIO.readInt(filer, "propsLength", stackBuffer);
        MiruTermId[][] values = new MiruTermId[fieldsLength][];
        MiruIBA[][] props = new MiruIBA[propsLength][];
        for (int i = 0; i < fieldsLength + propsLength; i++) {
            FilerIO.readInt(filer, "offest", stackBuffer);
        }
        for (int i = 0; i < fieldsLength; i++) {
            values[i] = valuesFromFiler(filer, stackBuffer);
        }
        for (int i = 0; i < propsLength; i++) {
            props[i] = propsFromFiler(filer, stackBuffer);
        }

        long time = FilerIO.readLong(filer, "time", stackBuffer);
        long version = FilerIO.readLong(filer, "version", stackBuffer);
        String[] authz = FilerIO.readStringArray(filer, "authz", stackBuffer);

        return new MiruInternalActivity(tenant, time, version, authz, values, props);
    }

    private MiruTermId[] valuesFromFiler(Filer filer, StackBuffer stackBuffer) throws IOException {
        int length = FilerIO.readInt(filer, "length", stackBuffer);
        if (length <= 0) {
            return null;
        }
        byte[] bufferMiruTerm = null;
        MiruTermId[] terms = new MiruTermId[length];
        for (int i = 0; i < length; i++) {
            int l = FilerIO.readInt(filer, "length", stackBuffer);
            if (bufferMiruTerm == null || bufferMiruTerm.length < l) {
                bufferMiruTerm = new byte[l];
            }
            FilerIO.read(filer, bufferMiruTerm, 0, l);
            terms[i] = termInterner.intern(bufferMiruTerm, 0, l);
        }

        return terms;
    }

    private MiruIBA[] propsFromFiler(Filer filer, StackBuffer stackBuffer) throws IOException {
        int length = FilerIO.readInt(filer, "length", stackBuffer);
        if (length <= 0) {
            return null;
        }
        MiruIBA[] terms = new MiruIBA[length];
        for (int i = 0; i < length; i++) {
            int l = FilerIO.readInt(filer, "length", stackBuffer);
            byte[] bytes = new byte[l];
            FilerIO.read(filer, bytes);
            terms[i] = new MiruIBA(bytes);
        }

        return terms;
    }

    public byte[] toBytes(MiruSchema schema, MiruInternalActivity activity, StackBuffer stackBuffer) throws IOException {

        int fieldsLength = activity.fieldsValues.length;
        int propsLength = activity.propsValues.length;

        byte[][] valueBytes = new byte[fieldsLength + propsLength][];
        int[] offsetIndex = new int[fieldsLength + propsLength];
        // fieldsLength + propsLength + fieldsIndex * 4 + propsIndex * 4
        int offset = (4 + 4) + (fieldsLength * 4) + (propsLength * 4);
        for (int i = 0; i < fieldsLength; i++) {
            MiruFieldDefinition fieldDefinition = schema.getFieldDefinition(i);
            MiruTermId[] values = fieldDefinition.type.hasFeature(Feature.stored) ? activity.fieldsValues[i] : null;
            offsetIndex[i] = offset;
            valueBytes[i] = fieldValuesToBytes(values, stackBuffer);
            offset += valueBytes[i].length;
        }

        for (int i = 0; i < propsLength; i++) {
            int ii = i + fieldsLength;
            offsetIndex[ii] = offset;
            valueBytes[ii] = fieldValuesToBytes(activity.propsValues[i], stackBuffer);
            offset += valueBytes[ii].length;
        }

        ByteArrayFiler filer = new ByteArrayFiler();
        FilerIO.writeInt(filer, fieldsLength, "fieldsLength", stackBuffer);
        FilerIO.writeInt(filer, propsLength, "propsLength", stackBuffer);
        for (int index : offsetIndex) {
            FilerIO.writeInt(filer, index, "index", stackBuffer);
        }
        for (byte[] value : valueBytes) {
            FilerIO.write(filer, value);
        }

        FilerIO.writeLong(filer, activity.time, "time", stackBuffer);
        FilerIO.writeLong(filer, activity.version, "version", stackBuffer);
        FilerIO.writeStringArray(filer, activity.authz, "authz", stackBuffer);
        return filer.getBytes();
    }

    private byte[] fieldValuesToBytes(MiruIBA[] values, StackBuffer stackBuffer) throws IOException {
        ByteArrayFiler filer = new ByteArrayFiler();
        if (values == null) {
            FilerIO.writeInt(filer, -1, "length", stackBuffer);
        } else {
            FilerIO.writeInt(filer, values.length, "length", stackBuffer);
            for (MiruIBA v : values) {
                byte[] bytes = v.immutableBytes();
                FilerIO.writeInt(filer, bytes.length, "length", stackBuffer);
                FilerIO.write(filer, bytes);
            }
        }
        return filer.getBytes();
    }

}
