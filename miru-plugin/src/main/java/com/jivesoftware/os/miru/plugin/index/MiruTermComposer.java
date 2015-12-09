package com.jivesoftware.os.miru.plugin.index;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Bytes;
import com.jivesoftware.os.filer.io.ByteArrayFiler;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.MiruInterner;
import com.jivesoftware.os.rcvs.marshall.api.UtilLexMarshaller;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 *
 */
public class MiruTermComposer {

    private final Charset charset;
    private final MiruInterner<MiruTermId> termInterner;

    public MiruTermComposer(final Charset charset, MiruInterner<MiruTermId> termInterner) {
        this.charset = charset;
        this.termInterner = termInterner;
    }

    public MiruTermId compose(MiruSchema schema, MiruFieldDefinition fieldDefinition, StackBuffer stackBuffer, String... parts) throws Exception {
        return termInterner.intern(compose(schema, fieldDefinition, stackBuffer, parts, 0, parts.length));
    }

    private byte[] compose(MiruSchema schema,
        MiruFieldDefinition fieldDefinition,
        StackBuffer stackBuffer,
        String[] parts,
        int offset,
        int length) throws Exception {

        MiruFieldDefinition[] compositeFieldDefinitions = schema.getCompositeFieldDefinitions(fieldDefinition.fieldId);
        if (compositeFieldDefinitions != null) {
            ByteArrayFiler filer = new ByteArrayFiler(new byte[length * (4 + 1)]); // minimal predicted size
            for (int i = 0; i < length; i++) {
                //TODO optimize
                byte[] bytes = composeBytes(compositeFieldDefinitions[i].prefix, parts[offset + i]);
                // all but last part are length prefixed
                if (i < compositeFieldDefinitions.length - 1) {
                    FilerIO.writeInt(filer, bytes.length, "length", stackBuffer);
                }
                filer.write(bytes);
            }
            return filer.getBytes();
        } else {
            return composeBytes(fieldDefinition.prefix, parts[offset]);
        }
    }

    private byte[] composeBytes(MiruFieldDefinition.Prefix p, String part) {
        byte[] termBytes;
        if (p != null && p.type.isAnalyzed()) {
            int sepIndex = part.indexOf(p.separator);
            if (sepIndex < 0) {
                throw new IllegalArgumentException("Term missing separator: " + part);
            }

            String pre = part.substring(0, sepIndex);
            String suf = part.substring(sepIndex + 1);
            byte[] sufBytes = suf.getBytes(charset);

            termBytes = new byte[p.length + sufBytes.length];
            writePrefixBytes(p, pre, termBytes);
            System.arraycopy(sufBytes, 0, termBytes, p.length, sufBytes.length);
        } else {
            termBytes = part.getBytes(charset);
        }
        return termBytes;
    }

    public String[] decompose(MiruSchema schema, MiruFieldDefinition fieldDefinition, StackBuffer stackBuffer, MiruTermId term) throws IOException {
        MiruFieldDefinition[] compositeFieldDefinitions = schema.getCompositeFieldDefinitions(fieldDefinition.fieldId);
        if (compositeFieldDefinitions != null) {
            String[] parts = new String[compositeFieldDefinitions.length];
            byte[] termBytes = term.getBytes();
            ByteArrayFiler filer = new ByteArrayFiler(termBytes);
            for (int i = 0; i < compositeFieldDefinitions.length; i++) {
                //TODO optimize
                // all but last part are length prefixed
                if (i < compositeFieldDefinitions.length - 1) {
                    int length = FilerIO.readInt(filer, "length", stackBuffer);
                    parts[i] = decomposeBytes(compositeFieldDefinitions[i], termBytes, (int) filer.getFilePointer(), length);
                    filer.skip(length);
                } else {
                    parts[i] = decomposeBytes(compositeFieldDefinitions[i], termBytes, (int) filer.getFilePointer(),
                        (int) (filer.length() - filer.getFilePointer()));
                }
            }
            return parts;
        } else {
            byte[] bytes = term.getBytes();
            return new String[] { decomposeBytes(fieldDefinition, bytes, 0, bytes.length) };
        }
    }

    private String decomposeBytes(MiruFieldDefinition fieldDefinition, byte[] termBytes, int offset, int length) {
        MiruFieldDefinition.Prefix p = fieldDefinition.prefix;
        if (p != null && p.type.isAnalyzed()) {
            String pre = readPrefixBytes(p, termBytes, offset);
            String suf = new String(termBytes, offset + p.length, length - p.length, charset);
            return pre + (char) p.separator + suf;
        } else {
            return new String(termBytes, offset, length, charset);
        }
    }

    private void writePrefixBytes(MiruFieldDefinition.Prefix p, String pre, byte[] termBytes) {
        if (p.type == MiruFieldDefinition.Prefix.Type.raw) {
            byte[] preBytes = pre.getBytes(charset);
            // one byte for length
            if (preBytes.length > (p.length - 1)) {
                throw new IllegalArgumentException("Prefix overflow: " + preBytes.length + " > " + (p.length - 1) + " (did you forget 1 byte for length?)");
            }
            System.arraycopy(preBytes, 0, termBytes, 0, preBytes.length);
            termBytes[p.length - 1] = (byte) preBytes.length;
        } else if (p.type == MiruFieldDefinition.Prefix.Type.numeric) {
            if (p.length == 4) {
                int v = Integer.parseInt(pre);
                byte[] vBytes = UtilLexMarshaller.intToLex(v); //TODO would be nice to marshal to dest array
                System.arraycopy(vBytes, 0, termBytes, 0, 4);
            } else if (p.length == 8) {
                long v = Long.parseLong(pre);
                byte[] vBytes = UtilLexMarshaller.longToLex(v); //TODO would be nice to marshal to dest array
                System.arraycopy(vBytes, 0, termBytes, 0, 8);
            } else {
                throw new IllegalStateException("Numeric prefix only supports int and long");
            }
        } else {
            throw new IllegalArgumentException("No prefix");
        }
    }

    private String readPrefixBytes(MiruFieldDefinition.Prefix p, byte[] termBytes, int offset) {
        if (p.type == MiruFieldDefinition.Prefix.Type.raw) {
            int length = (int) termBytes[offset + p.length - 1];
            return new String(termBytes, offset, length, charset);
        } else if (p.type == MiruFieldDefinition.Prefix.Type.numeric) {
            if (p.length == 4) {
                int value = UtilLexMarshaller.intFromLex(termBytes, offset);
                return String.valueOf(value);
            } else if (p.length == 8) {
                long value = UtilLexMarshaller.longFromLex(termBytes, offset);
                return String.valueOf(value);
            } else {
                throw new IllegalStateException("Numeric prefix only supports int and long");
            }
        } else {
            throw new IllegalArgumentException("No prefix");
        }
    }

    public byte[] prefixLowerInclusive(MiruSchema schema, MiruFieldDefinition fieldDefinition, StackBuffer stackBuffer, String... parts) throws Exception {
        MiruFieldDefinition[] compositeFieldDefinitions = schema.getCompositeFieldDefinitions(fieldDefinition.fieldId);
        if (compositeFieldDefinitions != null) {
            Preconditions.checkArgument(parts.length <= compositeFieldDefinitions.length,
                "Provided more value parts than we have composite field definitions");
            //TODO optimize
            int tailIndex = Math.min(compositeFieldDefinitions.length - 1, parts.length);
            byte[] headBytes = compose(schema, fieldDefinition, stackBuffer, parts, 0, tailIndex);
            if (parts.length == compositeFieldDefinitions.length) {
                byte[] tailBytes = prefixLowerInclusiveBytes(fieldDefinition, parts[parts.length - 1]);
                return Bytes.concat(headBytes, tailBytes);
            } else {
                return headBytes;
            }
        } else {
            Preconditions.checkArgument(parts.length == 1, "Provided multiple value parts for a non-composite field");
            return prefixLowerInclusiveBytes(fieldDefinition, parts[0]);
        }
    }

    private byte[] prefixLowerInclusiveBytes(MiruFieldDefinition fieldDefinition, String pre) {
        MiruFieldDefinition.Prefix p = fieldDefinition.prefix;
        if (p.type == MiruFieldDefinition.Prefix.Type.raw || p.type == MiruFieldDefinition.Prefix.Type.wildcard) {
            return pre.getBytes(charset);
        } else if (p.type == MiruFieldDefinition.Prefix.Type.numeric) {
            int v = Integer.parseInt(pre);
            return UtilLexMarshaller.intToLex(v);
        } else {
            throw new IllegalArgumentException("Can't range filter this field!");
        }
    }

    public byte[] prefixUpperExclusive(MiruSchema schema, MiruFieldDefinition fieldDefinition, StackBuffer stackBuffer, String... parts) throws Exception {
        MiruFieldDefinition[] compositeFieldDefinitions = schema.getCompositeFieldDefinitions(fieldDefinition.fieldId);
        if (compositeFieldDefinitions != null) {
            Preconditions.checkArgument(parts.length <= compositeFieldDefinitions.length,
                "Provided more value parts than we have composite field definitions");
            //TODO optimize
            int tailIndex = Math.min(compositeFieldDefinitions.length - 1, parts.length);
            byte[] headBytes = compose(schema, fieldDefinition, stackBuffer, parts, 0, tailIndex);
            if (parts.length == compositeFieldDefinitions.length) {
                byte[] tailBytes = prefixUpperExclusiveBytes(fieldDefinition, parts[parts.length - 1]);
                return Bytes.concat(headBytes, tailBytes);
            } else {
                return headBytes;
            }
        } else {
            Preconditions.checkArgument(parts.length == 1, "Provided multiple value parts for a non-composite field");
            return prefixUpperExclusiveBytes(fieldDefinition, parts[0]);
        }
    }

    public byte[] prefixUpperExclusiveBytes(MiruFieldDefinition fieldDefinition, String pre) {
        MiruFieldDefinition.Prefix p = fieldDefinition.prefix;
        if (p.type == MiruFieldDefinition.Prefix.Type.wildcard) {
            byte[] raw = pre.getBytes(charset);
            for (int i = raw.length - 1; i >= 0; i--) {
                if (raw[i] == Byte.MAX_VALUE) {
                    raw[i] = Byte.MIN_VALUE;
                } else {
                    raw[i]++;
                    break;
                }
            }
            return raw;
        } else if (p.type == MiruFieldDefinition.Prefix.Type.raw) {
            byte[] preBytes = pre.getBytes(charset);
            byte[] raw = new byte[p.length];
            System.arraycopy(preBytes, 0, raw, 0, preBytes.length);

            // given: [64,72,96,127]
            // want: [64,72,97,-128]
            for (int i = raw.length - 1; i >= 0; i--) {
                if (raw[i] == Byte.MAX_VALUE) {
                    raw[i] = Byte.MIN_VALUE;
                } else {
                    raw[i]++;
                    break;
                }
            }
            return raw;
        } else if (p.type == MiruFieldDefinition.Prefix.Type.numeric) {
            int v = Integer.parseInt(pre) + 1;
            return UtilLexMarshaller.intToLex(v);
        } else {
            throw new IllegalArgumentException("Can't range filter this field!");
        }
    }
}
