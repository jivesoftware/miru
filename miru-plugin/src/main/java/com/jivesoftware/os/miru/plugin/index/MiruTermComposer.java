package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.rcvs.marshall.api.UtilLexMarshaller;
import java.nio.charset.Charset;
import java.util.Collection;

/**
 *
 */
public class MiruTermComposer {

    private final Charset charset;

    public MiruTermComposer(final Charset charset) {
        this.charset = charset;
    }

    public MiruTermId compose(MiruFieldDefinition fieldDefinition, String value) {
        MiruFieldDefinition.Prefix p = fieldDefinition.prefix;
        if (p != null && p.type.isAnalyzed()) {
            int sepIndex = value.indexOf(p.separator);
            if (sepIndex < 0) {
                throw new IllegalArgumentException("Term missing separator: " + value);
            }

            String pre = value.substring(0, sepIndex);
            String suf = value.substring(sepIndex + 1);
            byte[] sufBytes = suf.getBytes(charset);

            byte[] termBytes = new byte[p.length + sufBytes.length];
            writePrefixBytes(p, pre, termBytes);
            System.arraycopy(sufBytes, 0, termBytes, p.length, sufBytes.length);
            return new MiruTermId(termBytes);
        } else {
            return new MiruTermId(value.getBytes(charset));
        }
    }

    public MiruTermId[] composeAll(MiruFieldDefinition fieldDefinition, Collection<String> values) {
        MiruTermId[] terms = new MiruTermId[values.size()];
        int i = 0;
        for (String value : values) {
            terms[i] = compose(fieldDefinition, value);
            i++;
        }
        return terms;
    }

    public String decompose(MiruFieldDefinition fieldDefinition, MiruTermId term) {
        MiruFieldDefinition.Prefix p = fieldDefinition.prefix;
        byte[] termBytes = term.getBytes();
        if (p != null && p.type.isAnalyzed()) {
            String pre = readPrefixBytes(p, termBytes);
            String suf = new String(termBytes, p.length, termBytes.length - p.length, charset);
            return pre + (char) p.separator + suf;
        } else {
            return new String(termBytes, charset);
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

    private String readPrefixBytes(MiruFieldDefinition.Prefix p, byte[] termBytes) {
        if (p.type == MiruFieldDefinition.Prefix.Type.raw) {
            int length = (int) termBytes[p.length - 1];
            return new String(termBytes, 0, length, charset);
        } else if (p.type == MiruFieldDefinition.Prefix.Type.numeric) {
            if (p.length == 4) {
                int value = UtilLexMarshaller.intFromLex(termBytes, 0);
                return String.valueOf(value);
            } else if (p.length == 8) {
                long value = UtilLexMarshaller.longFromLex(termBytes, 0);
                return String.valueOf(value);
            } else {
                throw new IllegalStateException("Numeric prefix only supports int and long");
            }
        } else {
            throw new IllegalArgumentException("No prefix");
        }
    }

    public byte[] prefixLowerInclusive(MiruFieldDefinition.Prefix p, String pre) {
        if (p.type == MiruFieldDefinition.Prefix.Type.raw || p.type == MiruFieldDefinition.Prefix.Type.wildcard) {
            return pre.getBytes(charset);
        } else if (p.type == MiruFieldDefinition.Prefix.Type.numeric) {
            int v = Integer.parseInt(pre);
            return UtilLexMarshaller.intToLex(v);
        } else {
            throw new IllegalArgumentException("Can't range filter this field!");
        }
    }

    public byte[] prefixUpperExclusive(MiruFieldDefinition.Prefix p, String pre) {
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
