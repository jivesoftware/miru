/*
 * Copyright 2013 Jive Software, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.jivesoftware.os.miru.reco.plugins;

import de.ruedigermoeller.serialization.FSTBasicObjectSerializer;
import de.ruedigermoeller.serialization.FSTConfiguration;
import de.ruedigermoeller.serialization.FSTObjectInput;
import de.ruedigermoeller.serialization.FSTObjectOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 *
 */
public class FstMarshaller {

    private final FSTConfiguration fstConfig;

    public FstMarshaller(FSTConfiguration fstConfig) {
        this.fstConfig = fstConfig;
    }

    public void registerSerializer(Class clazz, FSTBasicObjectSerializer serializer) {
        fstConfig.registerSerializer(clazz, serializer, true);
    }

    public <V extends Serializable> void serialize(V toSerialize, OutputStream outputStream) throws IOException {
        FSTObjectOutput out = fstConfig.getObjectOutput(outputStream);
        out.writeObject(toSerialize, toSerialize.getClass());
        out.flush();

    }

    public <V> V deserialize(InputStream inputStream, Class<V> clazz) throws Exception {
        FSTObjectInput in = fstConfig.getObjectInput(inputStream);
        return (V) in.readObject(clazz);
    }
}
