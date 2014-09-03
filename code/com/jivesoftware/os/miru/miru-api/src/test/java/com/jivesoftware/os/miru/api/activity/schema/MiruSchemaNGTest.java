/*
 * Copyright 2014 jonathan.colt.
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

package com.jivesoftware.os.miru.api.activity.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class MiruSchemaNGTest {

    @Test
    public void testSerDes() throws JsonProcessingException, IOException {

        MiruSchema write = new MiruSchema(DefaultMiruSchemaDefinition.FIELDS, DefaultMiruSchemaDefinition.PROPERTIES);
        ObjectMapper objectMapper = new ObjectMapper();
        File schemaFile = File.createTempFile("ser", "des");
        objectMapper.writeValue(schemaFile, write);
        MiruSchema read = objectMapper.readValue(schemaFile, MiruSchema.class);
        Assert.assertEquals(objectMapper.writeValueAsString(write), objectMapper.writeValueAsString(read));
    }

}
