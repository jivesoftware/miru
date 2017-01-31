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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition.Prefix;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition.Type;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema.Builder;
import java.io.File;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * @author jonathan.colt
 */
public class MiruSchemaNGTest {

    @Test
    public void testSerDes() throws IOException {

        MiruSchema write = new MiruSchema.Builder("test", 1)
            .setFieldDefinitions(DefaultMiruSchemaDefinition.FIELDS)
            .setPropertyDefinitions(DefaultMiruSchemaDefinition.PROPERTIES)
            .build();
        ObjectMapper objectMapper = new ObjectMapper();
        File schemaFile = File.createTempFile("ser", "des");
        objectMapper.writeValue(schemaFile, write);
        MiruSchema read = objectMapper.readValue(schemaFile, MiruSchema.class);
        Assert.assertEquals(objectMapper.writeValueAsString(write), objectMapper.writeValueAsString(read));
    }

    @Test
    public void testUnchanged() throws Exception {
        MiruSchema a = serdes(new Builder("test", 1)
            .setFieldDefinitions(new MiruFieldDefinition[] {
                new MiruFieldDefinition(0, "a", Type.singleTerm, Prefix.NONE),
                new MiruFieldDefinition(1, "b", Type.singleTerm, Prefix.NONE),
                new MiruFieldDefinition(2, "c", Type.singleTerm, Prefix.NONE)
            })
            .build());
        MiruSchema b = serdes(new Builder("test", 1)
            .setFieldDefinitions(new MiruFieldDefinition[] {
                new MiruFieldDefinition(0, "a", Type.singleTerm, Prefix.NONE),
                new MiruFieldDefinition(1, "b", Type.singleTerm, Prefix.NONE),
                new MiruFieldDefinition(2, "c", Type.singleTerm, Prefix.NONE)
            })
            .build());
        assertTrue(MiruSchema.checkEquals(a, b));
        assertTrue(MiruSchema.deepEquals(a, b));
        assertTrue(MiruSchema.checkAdditive(a, b));
    }

    @Test
    public void testRemovedField() throws Exception {
        MiruSchema a = serdes(new Builder("test", 1)
            .setFieldDefinitions(new MiruFieldDefinition[] {
                new MiruFieldDefinition(0, "a", Type.singleTerm, Prefix.NONE),
                new MiruFieldDefinition(1, "b", Type.singleTerm, Prefix.NONE),
                new MiruFieldDefinition(2, "c", Type.singleTerm, Prefix.NONE)
            })
            .build());
        MiruSchema b = serdes(new Builder("test", 1)
            .setFieldDefinitions(new MiruFieldDefinition[] {
                new MiruFieldDefinition(0, "a", Type.singleTerm, Prefix.NONE),
                new MiruFieldDefinition(1, "b", Type.nonIndexedNonStored, Prefix.NONE),
                new MiruFieldDefinition(2, "c", Type.singleTerm, Prefix.NONE)
            })
            .build());
        assertTrue(MiruSchema.checkEquals(a, b));
        assertFalse(MiruSchema.deepEquals(a, b));
        assertTrue(MiruSchema.checkAdditive(a, b));
    }

    @Test
    public void testWrongName() throws Exception {
        MiruSchema a = serdes(new Builder("test", 1)
            .setFieldDefinitions(new MiruFieldDefinition[] {
                new MiruFieldDefinition(0, "a", Type.singleTerm, Prefix.NONE),
                new MiruFieldDefinition(1, "b", Type.singleTerm, Prefix.NONE),
                new MiruFieldDefinition(2, "c", Type.singleTerm, Prefix.NONE)
            })
            .build());
        MiruSchema b = serdes(new Builder("test", 1)
            .setFieldDefinitions(new MiruFieldDefinition[] {
                new MiruFieldDefinition(0, "a", Type.singleTerm, Prefix.NONE),
                new MiruFieldDefinition(1, "bb", Type.singleTerm, Prefix.NONE),
                new MiruFieldDefinition(2, "c", Type.singleTerm, Prefix.NONE)
            })
            .build());
        assertTrue(MiruSchema.checkEquals(a, b));
        assertFalse(MiruSchema.deepEquals(a, b));
        assertFalse(MiruSchema.checkAdditive(a, b));
    }

    @Test
    public void testWrongPrefix() throws Exception {
        MiruSchema a = serdes(new Builder("test", 1)
            .setFieldDefinitions(new MiruFieldDefinition[] {
                new MiruFieldDefinition(0, "a", Type.singleTerm, Prefix.NONE),
                new MiruFieldDefinition(1, "b", Type.singleTerm, Prefix.WILDCARD),
                new MiruFieldDefinition(2, "c", Type.singleTerm, Prefix.NONE)
            })
            .build());
        MiruSchema b = serdes(new Builder("test", 1)
            .setFieldDefinitions(new MiruFieldDefinition[] {
                new MiruFieldDefinition(0, "a", Type.singleTerm, Prefix.NONE),
                new MiruFieldDefinition(1, "bb", Type.singleTerm, Prefix.NONE),
                new MiruFieldDefinition(2, "c", Type.singleTerm, Prefix.NONE)
            })
            .build());
        assertTrue(MiruSchema.checkEquals(a, b));
        assertFalse(MiruSchema.deepEquals(a, b));
        assertFalse(MiruSchema.checkAdditive(a, b));
    }

    @Test
    public void testNonAdditive() throws Exception {
        MiruSchema a = serdes(new Builder("test", 1)
            .setFieldDefinitions(new MiruFieldDefinition[] {
                new MiruFieldDefinition(0, "a", Type.singleTerm, Prefix.NONE),
                new MiruFieldDefinition(1, "b", Type.singleTerm, Prefix.NONE),
                new MiruFieldDefinition(2, "c", Type.singleTerm, Prefix.NONE)
            })
            .build());
        MiruSchema b = serdes(new Builder("test", 1)
            .setFieldDefinitions(new MiruFieldDefinition[] {
                new MiruFieldDefinition(0, "a", Type.singleTerm, Prefix.NONE),
                new MiruFieldDefinition(1, "b", Type.nonIndexed, Prefix.NONE),
                new MiruFieldDefinition(2, "c", Type.singleTerm, Prefix.NONE)
            })
            .build());
        assertTrue(MiruSchema.checkEquals(a, b));
        assertFalse(MiruSchema.deepEquals(a, b));
        assertFalse(MiruSchema.checkAdditive(a, b));
    }

    @Test
    public void testMissingField() throws Exception {
        MiruSchema a = serdes(new Builder("test", 1)
            .setFieldDefinitions(new MiruFieldDefinition[] {
                new MiruFieldDefinition(0, "a", Type.singleTerm, Prefix.NONE),
                new MiruFieldDefinition(1, "b", Type.singleTerm, Prefix.NONE),
                new MiruFieldDefinition(2, "c", Type.singleTerm, Prefix.NONE)
            })
            .build());
        MiruSchema b = serdes(new Builder("test", 1)
            .setFieldDefinitions(new MiruFieldDefinition[] {
                new MiruFieldDefinition(0, "a", Type.singleTerm, Prefix.NONE),
                new MiruFieldDefinition(1, "b", Type.singleTerm, Prefix.NONE)
            })
            .build());
        assertTrue(MiruSchema.checkEquals(a, b));
        assertFalse(MiruSchema.deepEquals(a, b));
        assertFalse(MiruSchema.checkAdditive(a, b));
    }

    @Test
    public void testAddField() throws Exception {
        MiruSchema a = serdes(new Builder("test", 1)
            .setFieldDefinitions(new MiruFieldDefinition[] {
                new MiruFieldDefinition(0, "a", Type.singleTerm, Prefix.NONE),
                new MiruFieldDefinition(1, "b", Type.singleTerm, Prefix.NONE),
                new MiruFieldDefinition(2, "c", Type.singleTerm, Prefix.NONE)
            })
            .build());
        MiruSchema b = serdes(new Builder("test", 1)
            .setFieldDefinitions(new MiruFieldDefinition[] {
                new MiruFieldDefinition(0, "a", Type.singleTerm, Prefix.NONE),
                new MiruFieldDefinition(1, "b", Type.singleTerm, Prefix.NONE),
                new MiruFieldDefinition(2, "c", Type.singleTerm, Prefix.NONE),
                new MiruFieldDefinition(3, "d", Type.singleTerm, Prefix.NONE)
            })
            .build());
        assertTrue(MiruSchema.checkEquals(a, b));
        assertFalse(MiruSchema.deepEquals(a, b));
        assertTrue(MiruSchema.checkAdditive(a, b));
    }

    @Test
    public void testComposite() throws Exception {
        MiruSchema schema1 = new Builder("test", 1)
            .setFieldDefinitions(new MiruFieldDefinition[] {
                new MiruFieldDefinition(0, "a", Type.singleTerm, Prefix.NONE),
                new MiruFieldDefinition(1, "b", Type.singleTerm, Prefix.NONE),
                new MiruFieldDefinition(2, "c", Type.singleTerm, Prefix.NONE)
            })
            .setComposite(ImmutableMap.<String, String[]>builder()
                .put("d", new String[] { "a", "b" })
                .put("e", new String[] { "b", "c" })
                .put("f", new String[] { "a", "c" })
                .build()
            )
            .build();
        MiruSchema schema2 = new Builder("test", 1)
            .setFieldDefinitions(new MiruFieldDefinition[] {
                new MiruFieldDefinition(0, "a", Type.singleTerm, Prefix.NONE),
                new MiruFieldDefinition(1, "b", Type.singleTerm, Prefix.NONE),
                new MiruFieldDefinition(2, "c", Type.singleTerm, Prefix.NONE)
            })
            .setComposite(ImmutableMap.<String, String[]>builder()
                .put("d", new String[] { "a", "b" })
                .put("e", new String[] { "b", "c" })
                .build()
            )
            .build();

        MiruSchema a = serdes(schema1);
        MiruSchema b = serdes(schema1);
        assertTrue(MiruSchema.checkEquals(a, b));
        assertTrue(MiruSchema.deepEquals(a, b));
        MiruSchema c = serdes(schema2);
        assertTrue(MiruSchema.checkEquals(a, c));
        assertFalse(MiruSchema.deepEquals(a, c));

    }

    private ObjectMapper objectMapper = new ObjectMapper();

    private MiruSchema serdes(MiruSchema schema) throws Exception {
        return objectMapper.readValue(objectMapper.writeValueAsString(schema), MiruSchema.class);
    }

}
