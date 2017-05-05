package cn.whaley.bi.logsys.common;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;


/**
 * Created by fj on 17/4/22.
 */
public class StringDecoder {

    public StringDecoder() {
        this.decodeQuotedBackslash = false;
    }

    public StringDecoder(boolean decodeQuotedBackslash) {
        this.decodeQuotedBackslash = decodeQuotedBackslash;
    }

    /**
     * 是否解码双引号中的反斜杠
     */
    private boolean decodeQuotedBackslash;

    public boolean isDecodeQuotedBackslash() {
        return decodeQuotedBackslash;
    }

    public void setDecodeQuotedBackslash(boolean decodeQuotedBackslash) {
        this.decodeQuotedBackslash = decodeQuotedBackslash;
    }


    /**
     * 解析nginx编码字符串为字符串对象
     *
     * @param str
     * @return
     */
    public String decodeToString(String str) {
        byte[] bytes = decodeToBytes(str);
        try {
            return new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * 解析nginx风格编码字符对象为字节UTF-8数组
     *
     * @param str
     * @return
     */
    public byte[] decodeToBytes(String str) {
        return decodeToBytes(str.getBytes());
    }

    public byte[] decodeToBytes(byte[] bytes) {
        //分配一个足够大的缓存空间
        ByteBuffer buffer = ByteBuffer.allocate(bytes.length * 3);
        int len = bytes.length;
        int end = len - 3;
        int skip = 0;
        boolean inQuoted = false;
        for (int i = 0; i < len; i++) {
            if (skip > 0) {
                skip = skip - 1;
                continue;
            }
            if (bytes[i] == '"') {
                inQuoted = !inQuoted;
            }

            if (i < end && bytes[i] == '\\') {
                if (bytes[i + 1] == '\\') {
                    buffer.put(bytes[i]);
                    buffer.put(bytes[i + 1]);
                    skip = 1;
                } else if ((bytes[i + 1] == 'x' && isHexByte(bytes[i + 2]) && isHexByte(bytes[i + 3]))) {
                    if (inQuoted && decodeQuotedBackslash) {
                        buffer.put(bytes[i]);
                        buffer.put(bytes[i]);
                        buffer.put(bytes[i + 1]);
                        buffer.put(bytes[i + 2]);
                        buffer.put(bytes[i + 3]);
                    } else {
                        buffer.put(pairToByte(bytes[i + 2], bytes[i + 3]));
                    }
                    skip = 3;
                }
                continue;
            }
            buffer.put(bytes[i]);
            skip = 0;

        }
        buffer.flip();
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result, 0, result.length);
        return result;
    }

    private Boolean isHexByte(byte chr) {
        return ((chr >= 65 && chr <= 70)
                || (chr >= 97 && chr <= 102)
                || (chr >= 48 && chr <= 57)
        );
    }

    private byte hexByteToByte(byte chr) {
        //A=65,F=70,a=97,f=102,0=48,9=57
        switch (chr) {
            case 65:
                return 10;
            case 66:
                return 11;
            case 67:
                return 12;
            case 68:
                return 13;
            case 69:
                return 14;
            case 70:
                return 15;
            case 97:
                return 10;
            case 98:
                return 11;
            case 99:
                return 12;
            case 100:
                return 13;
            case 101:
                return 14;
            case 102:
                return 15;
            case 48:
                return 0;
            case 49:
                return 1;
            case 50:
                return 2;
            case 51:
                return 3;
            case 52:
                return 4;
            case 53:
                return 5;
            case 54:
                return 6;
            case 55:
                return 7;
            case 56:
                return 8;
            case 57:
                return 9;
            default:
                throw new UnsupportedOperationException("invalid char " + chr);
        }
    }

    private byte pairToByte(byte high, byte low) {
        Integer intValue = (hexByteToByte(high) * 16 + hexByteToByte(low));
        return intValue.byteValue();
    }

}

