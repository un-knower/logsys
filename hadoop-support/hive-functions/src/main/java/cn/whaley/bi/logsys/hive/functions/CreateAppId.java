package cn.whaley.bi.logsys.hive.functions;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by fj on 17/6/5.
 */
@Description(name = "_FUNC_"
        , value = "_FUNC_(orgCode:string,productCode:string,appCode:string," +
        "orgCodeNoise:string[default ''],productCodeNoise:string[default ''],appCodeNoise:string[default '']"
        , extended = "version:" + CreateAppId.VERSION
)
public class CreateAppId extends GenericUDF {

    public static final String VERSION = "1.0";

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        //参数至少为3个
        if (arguments.length < 3) {
            throw new UDFArgumentException("required 3 or more parameters.");
        }
        //第4~6个参数分别为 orgCodeNoise   productCodeNoise   appCodeNoise
        for (int i = 3; i < arguments.length; i++) {
            ObjectInspector oi = arguments[i];
            if (!(oi instanceof ConstantObjectInspector)) {
                throw new UDFArgumentTypeException(i, i + "  must be a constant value, " + oi.getTypeName() + " was passed.");
            }
            if (i == 3) {
                orgCodeNoise = ((ConstantObjectInspector) arguments[i]).getWritableConstantValue().toString();
            } else if (i == 4) {
                productCodeNoise = ((ConstantObjectInspector) arguments[i]).getWritableConstantValue().toString();
            } else if (i == 5) {
                appCodeNoise = ((ConstantObjectInspector) arguments[i]).getWritableConstantValue().toString();
            }
        }
        //函数返回值定义
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        String orgCode =  arguments[0].get().toString();
        String productCode =   arguments[1].get().toString();
        String appCode =   arguments[2].get().toString();
        String result = hashCodeForOrgId(orgCode) + hashCodeForProductCode(productCode) + hashCodeForAppCode(appCode);
        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        String functionName = getClass().getSimpleName().toLowerCase();
        return getStandardDisplayString(functionName, children, ",");
    }


    private static byte[] hex = "0123456789abcdef".getBytes();
    private static byte[] chars = new byte[32];

    static {
        for (int i = 0; i < 10; i++) {
            chars[i] = (byte) ('0' + i);
        }
        for (int i = 0; i < 22; i++) {
            chars[i + 10] = (byte) ('a' + i);
        }
    }


    private String orgCodeNoise = "";
    private String productCodeNoise = "";
    private String appCodeNoise = "";

    /**
     * 组织代码的16位长度哈希字符串
     *
     * @param orgId
     */
    private String hashCodeForOrgId(String orgId) {
        String md532 = getMD5Str32(orgId + orgCodeNoise);
        String part1 = hashCode16To8(md532.substring(0, 16));
        String part2 = hashCode16To8(md532.substring(16, 32));
        return part1 + part2;
    }

    /**
     * 产品代码的8位长度哈希字符串
     *
     * @param productCode
     * @return
     */
    private String hashCodeForProductCode(String productCode) {
        String hashCode16 = getMD5Str16(productCode + productCodeNoise);
        return hashCode16To8(hashCode16);
    }

    /**
     * 应用代码的8位长度哈希字符串
     *
     * @param appCode
     * @return
     */
    private String hashCodeForAppCode(String appCode) {
        String hashCode16 = getMD5Str16(appCode + appCodeNoise);
        return hashCode16To8(hashCode16);
    }

    private String getMD5Str32(String str) {
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("md5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        byte[] md5 = digest.digest(str.getBytes());
        return bytes2HexString(md5);
    }

    /**
     * 获取value值的16位MD值
     *
     * @param str
     * @return
     */
    private String getMD5Str16(String str) {
        String md5Str32 = getMD5Str32(str);
        return md5Str32.substring(8, 24);
    }


    private String hashCode16To8(String hashCode16) {
        byte[] values = new byte[8];
        byte[] codeIndex = new byte[hashCode16.length()];

        hashCode16 = hashCode16.toLowerCase();
        byte[] bytes = hashCode16.getBytes();
        for (int i = 0; i < bytes.length; i++) {
            byte chr = bytes[i];
            if (chr >= '0' && chr <= '9') {
                codeIndex[i] = (byte) (chr - '0');
            } else {
                codeIndex[i] = (byte) (chr - 'a' + 10);
            }
        }
        for (int i = 0; i < 8; i++) {
            int index = codeIndex[i] + codeIndex[i + 8] + 1;
            values[i] = chars[index];
        }
        return new String(values);
    }

    /**
     * 获取字节数组的十六进字符串表示
     *
     * @param b
     * @return
     */
    private String bytes2HexString(byte[] b) {
        byte[] buff = new byte[2 * b.length];
        for (int i = 0; i < b.length; i++) {
            buff[2 * i] = hex[(b[i] >> 4) & 0x0f];
            buff[2 * i + 1] = hex[b[i] & 0x0f];
        }
        return new String(buff);
    }

}
