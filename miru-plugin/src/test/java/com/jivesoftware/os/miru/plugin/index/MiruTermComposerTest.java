package com.jivesoftware.os.miru.plugin.index;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.plugin.MiruInterner;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class MiruTermComposerTest {

    MiruInterner<MiruTermId> termInterner = new MiruInterner<MiruTermId>(true) {
        @Override
        public MiruTermId create(byte[] bytes) {
            return new MiruTermId(bytes);
        }
    };

    @Test
    public void testRawComposeDecompose() throws Exception {

        MiruTermComposer composer = new MiruTermComposer(Charsets.UTF_8, termInterner);
        MiruFieldDefinition fieldDefinition = new MiruFieldDefinition(0,
            "field1",
            MiruFieldDefinition.Type.singleTerm,
            new MiruFieldDefinition.Prefix(MiruFieldDefinition.Prefix.Type.raw, 5, '\t'));
        MiruSchema schema = new MiruSchema.Builder("test", 1).setFieldDefinitions(new MiruFieldDefinition[] { fieldDefinition }).build();
        StackBuffer stackBuffer = new StackBuffer();

        assertEquals(composer.decompose(schema, fieldDefinition, stackBuffer, composer.compose(schema, fieldDefinition, stackBuffer, "a\t123"))[0],
            "a\t123");
        assertEquals(composer.decompose(schema, fieldDefinition, stackBuffer, composer.compose(schema, fieldDefinition, stackBuffer, "ab\t123"))[0],
            "ab\t123");
        assertEquals(composer.decompose(schema, fieldDefinition, stackBuffer, composer.compose(schema, fieldDefinition, stackBuffer, "abc\t123"))[0],
            "abc\t123");
        assertEquals(composer.decompose(schema, fieldDefinition, stackBuffer, composer.compose(schema, fieldDefinition, stackBuffer, "abcd\t123"))[0],
            "abcd\t123");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testRawComposeDecompose_overflow() throws Exception {
        MiruTermComposer composer = new MiruTermComposer(Charsets.UTF_8, termInterner);
        MiruFieldDefinition fieldDefinition = new MiruFieldDefinition(0,
            "field1",
            MiruFieldDefinition.Type.singleTerm,
            new MiruFieldDefinition.Prefix(MiruFieldDefinition.Prefix.Type.raw, 5, '\t'));
        MiruSchema schema = new MiruSchema.Builder("test", 1).setFieldDefinitions(new MiruFieldDefinition[] { fieldDefinition }).build();
        StackBuffer stackBuffer = new StackBuffer();

        composer.decompose(schema, fieldDefinition, stackBuffer, composer.compose(schema, fieldDefinition, stackBuffer, "abcde\t123"));
    }

    @Test
    public void testNumericComposeDecompose() throws Exception {
        MiruTermComposer composer = new MiruTermComposer(Charsets.UTF_8, termInterner);
        MiruFieldDefinition fieldDefinition = new MiruFieldDefinition(0,
            "field1",
            MiruFieldDefinition.Type.singleTerm,
            new MiruFieldDefinition.Prefix(MiruFieldDefinition.Prefix.Type.numeric, 4, '\t'));
        MiruSchema schema = new MiruSchema.Builder("test", 1).setFieldDefinitions(new MiruFieldDefinition[] { fieldDefinition }).build();
        StackBuffer stackBuffer = new StackBuffer();

        assertEquals(composer.decompose(schema, fieldDefinition, stackBuffer, composer.compose(schema, fieldDefinition, stackBuffer, "1\t123"))[0],
            "1\t123");
        assertEquals(composer.decompose(schema, fieldDefinition, stackBuffer, composer.compose(schema, fieldDefinition, stackBuffer, "12\t123"))[0],
            "12\t123");
        assertEquals(composer.decompose(schema, fieldDefinition, stackBuffer, composer.compose(schema, fieldDefinition, stackBuffer, "123\t123"))[0],
            "123\t123");
        assertEquals(composer.decompose(schema, fieldDefinition, stackBuffer, composer.compose(schema, fieldDefinition, stackBuffer, "1234\t123"))[0],
            "1234\t123");
    }

    @Test(expectedExceptions = NumberFormatException.class)
    public void testNumericComposeDecompose_overflow() throws Exception {
        MiruTermComposer composer = new MiruTermComposer(Charsets.UTF_8, termInterner);
        MiruFieldDefinition fieldDefinition = new MiruFieldDefinition(0,
            "field1",
            MiruFieldDefinition.Type.singleTerm,
            new MiruFieldDefinition.Prefix(MiruFieldDefinition.Prefix.Type.numeric, 4, '\t'));
        MiruSchema schema = new MiruSchema.Builder("test", 1).setFieldDefinitions(new MiruFieldDefinition[] { fieldDefinition }).build();
        StackBuffer stackBuffer = new StackBuffer();

        composer.decompose(schema, fieldDefinition, stackBuffer, composer.compose(schema, fieldDefinition, stackBuffer, Long.MAX_VALUE + "\t123"));
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testNumericComposeDecompose_nonIntLong() throws Exception {
        MiruTermComposer composer = new MiruTermComposer(Charsets.UTF_8, termInterner);
        MiruFieldDefinition fieldDefinition = new MiruFieldDefinition(0,
            "field1",
            MiruFieldDefinition.Type.singleTerm,
            new MiruFieldDefinition.Prefix(MiruFieldDefinition.Prefix.Type.numeric, 6, '\t'));
        MiruSchema schema = new MiruSchema.Builder("test", 1).setFieldDefinitions(new MiruFieldDefinition[] { fieldDefinition }).build();
        StackBuffer stackBuffer = new StackBuffer();

        composer.decompose(schema, fieldDefinition, stackBuffer, composer.compose(schema, fieldDefinition, stackBuffer, "1234\t123"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNumericComposeDecompose_nonNumeric() throws Exception {
        MiruTermComposer composer = new MiruTermComposer(Charsets.UTF_8, termInterner);
        MiruFieldDefinition fieldDefinition = new MiruFieldDefinition(0,
            "field1",
            MiruFieldDefinition.Type.singleTerm,
            new MiruFieldDefinition.Prefix(MiruFieldDefinition.Prefix.Type.numeric, 4, '\t'));
        MiruSchema schema = new MiruSchema.Builder("test", 1).setFieldDefinitions(new MiruFieldDefinition[] { fieldDefinition }).build();
        StackBuffer stackBuffer = new StackBuffer();

        composer.decompose(schema, fieldDefinition, stackBuffer, composer.compose(schema, fieldDefinition, stackBuffer, "abcd\t123"));
    }

    @Test
    public void testCompositeField() throws Exception {

        MiruTermComposer composer = new MiruTermComposer(Charsets.UTF_8, termInterner);
        MiruFieldDefinition field1Definition = new MiruFieldDefinition(0,
            "field1",
            MiruFieldDefinition.Type.singleTerm,
            new MiruFieldDefinition.Prefix(MiruFieldDefinition.Prefix.Type.numeric, 4, ' '));
        MiruFieldDefinition field2Definition = new MiruFieldDefinition(1,
            "field2",
            MiruFieldDefinition.Type.singleTerm,
            new MiruFieldDefinition.Prefix(MiruFieldDefinition.Prefix.Type.raw, 2, ' '));
        MiruFieldDefinition field3Definition = new MiruFieldDefinition(2,
            "field3",
            MiruFieldDefinition.Type.singleTerm,
            MiruFieldDefinition.Prefix.WILDCARD);
        MiruSchema schema = new MiruSchema.Builder("test", 1)
            .setFieldDefinitions(new MiruFieldDefinition[] { field1Definition, field2Definition, field3Definition })
            .setComposite(ImmutableMap.of("field3", new String[] { "field1", "field2" }))
            .build();
        StackBuffer stackBuffer = new StackBuffer();

        assertEquals(
            new MiruValue(composer.decompose(schema, field3Definition, stackBuffer,
                composer.compose(schema, field3Definition, stackBuffer, "12 a", "b c"))),
            new MiruValue("12 a", "b c"));
        assertEquals(
            new MiruValue(composer.decompose(schema, field3Definition, stackBuffer,
                composer.compose(schema, field3Definition, stackBuffer, "23 b", "c d"))),
            new MiruValue("23 b", "c d"));
        assertEquals(
            new MiruValue(composer.decompose(schema, field3Definition, stackBuffer,
                composer.compose(schema, field3Definition, stackBuffer, "34 c", "d e"))),
            new MiruValue("34 c", "d e"));
    }
}
