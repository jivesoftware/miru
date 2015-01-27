package com.jivesoftware.os.miru.plugin.index;

import com.google.common.base.Charsets;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class MiruTermComposerTest {

    @Test
    public void testRawComposeDecompose() throws Exception {
        MiruTermComposer composer = new MiruTermComposer(Charsets.UTF_8);
        MiruFieldDefinition fieldDefinition = new MiruFieldDefinition(0,
            "field1",
            MiruFieldDefinition.Type.singleTerm,
            new MiruFieldDefinition.Prefix(MiruFieldDefinition.Prefix.Type.raw, 5, '\t'));

        assertEquals(composer.decompose(fieldDefinition, composer.compose(fieldDefinition, "a\t123")), "a\t123");
        assertEquals(composer.decompose(fieldDefinition, composer.compose(fieldDefinition, "ab\t123")), "ab\t123");
        assertEquals(composer.decompose(fieldDefinition, composer.compose(fieldDefinition, "abc\t123")), "abc\t123");
        assertEquals(composer.decompose(fieldDefinition, composer.compose(fieldDefinition, "abcd\t123")), "abcd\t123");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testRawComposeDecompose_overflow() throws Exception {
        MiruTermComposer composer = new MiruTermComposer(Charsets.UTF_8);
        MiruFieldDefinition fieldDefinition = new MiruFieldDefinition(0,
            "field1",
            MiruFieldDefinition.Type.singleTerm,
            new MiruFieldDefinition.Prefix(MiruFieldDefinition.Prefix.Type.raw, 5, '\t'));

        composer.decompose(fieldDefinition, composer.compose(fieldDefinition, "abcde\t123"));
    }

    @Test
    public void testNumericComposeDecompose() throws Exception {
        MiruTermComposer composer = new MiruTermComposer(Charsets.UTF_8);
        MiruFieldDefinition fieldDefinition = new MiruFieldDefinition(0,
            "field1",
            MiruFieldDefinition.Type.singleTerm,
            new MiruFieldDefinition.Prefix(MiruFieldDefinition.Prefix.Type.numeric, 4, '\t'));

        assertEquals(composer.decompose(fieldDefinition, composer.compose(fieldDefinition, "1\t123")), "1\t123");
        assertEquals(composer.decompose(fieldDefinition, composer.compose(fieldDefinition, "12\t123")), "12\t123");
        assertEquals(composer.decompose(fieldDefinition, composer.compose(fieldDefinition, "123\t123")), "123\t123");
        assertEquals(composer.decompose(fieldDefinition, composer.compose(fieldDefinition, "1234\t123")), "1234\t123");
    }

    @Test(expectedExceptions = NumberFormatException.class)
    public void testNumericComposeDecompose_overflow() throws Exception {
        MiruTermComposer composer = new MiruTermComposer(Charsets.UTF_8);
        MiruFieldDefinition fieldDefinition = new MiruFieldDefinition(0,
            "field1",
            MiruFieldDefinition.Type.singleTerm,
            new MiruFieldDefinition.Prefix(MiruFieldDefinition.Prefix.Type.numeric, 4, '\t'));

        composer.decompose(fieldDefinition, composer.compose(fieldDefinition, Long.MAX_VALUE + "\t123"));
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testNumericComposeDecompose_nonIntLong() throws Exception {
        MiruTermComposer composer = new MiruTermComposer(Charsets.UTF_8);
        MiruFieldDefinition fieldDefinition = new MiruFieldDefinition(0,
            "field1",
            MiruFieldDefinition.Type.singleTerm,
            new MiruFieldDefinition.Prefix(MiruFieldDefinition.Prefix.Type.numeric, 6, '\t'));

        composer.decompose(fieldDefinition, composer.compose(fieldDefinition, "1234\t123"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNumericComposeDecompose_nonNumeric() throws Exception {
        MiruTermComposer composer = new MiruTermComposer(Charsets.UTF_8);
        MiruFieldDefinition fieldDefinition = new MiruFieldDefinition(0,
            "field1",
            MiruFieldDefinition.Type.singleTerm,
            new MiruFieldDefinition.Prefix(MiruFieldDefinition.Prefix.Type.numeric, 4, '\t'));

        composer.decompose(fieldDefinition, composer.compose(fieldDefinition, "abcd\t123"));
    }

}