package cn.whaley.bi.logsys.metadata.service;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;

/**
 * Created by fj on 2017/6/27.
 */
public class HiveUtil {

    public static List<PrimitiveObjectInspector.PrimitiveCategory> numericTypeList = new ArrayList<PrimitiveObjectInspector.PrimitiveCategory>();
    // The ordering of types here is used to determine which numeric types
    // are common/convertible to one another. Probably better to rely on the
    // ordering explicitly defined here than to assume that the enum values
    // that were arbitrarily assigned in PrimitiveCategory work for our purposes.
    public static EnumMap<PrimitiveObjectInspector.PrimitiveCategory, Integer> numericTypes =
            new EnumMap<>(PrimitiveObjectInspector.PrimitiveCategory.class);

    static {
        registerNumericType(PrimitiveObjectInspector.PrimitiveCategory.BYTE, 1);
        registerNumericType(PrimitiveObjectInspector.PrimitiveCategory.SHORT, 2);
        registerNumericType(PrimitiveObjectInspector.PrimitiveCategory.INT, 3);
        registerNumericType(PrimitiveObjectInspector.PrimitiveCategory.LONG, 4);
        registerNumericType(PrimitiveObjectInspector.PrimitiveCategory.FLOAT, 5);
        registerNumericType(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE, 6);
        registerNumericType(PrimitiveObjectInspector.PrimitiveCategory.DECIMAL, 7);
        registerNumericType(PrimitiveObjectInspector.PrimitiveCategory.STRING, 8);
    }


    public static boolean implicitConvertible(String from, String to) {
        return HiveUtil.implicitConvertible(TypeInfoUtils.getTypeInfoFromTypeString(from),
                TypeInfoUtils.getTypeInfoFromTypeString(to));
    }

    public static boolean implicitConvertible(TypeInfo from, TypeInfo to) {
        if (from.equals(to)) {
            return true;
        }

        // Reimplemented to use PrimitiveCategory rather than TypeInfo, because
        // 2 TypeInfos from the same qualified type (varchar, decimal) should still be
        // seen as equivalent.
        if (from.getCategory() == ObjectInspector.Category.PRIMITIVE && to.getCategory() == ObjectInspector.Category.PRIMITIVE) {
            return implicitConvertible(
                    ((PrimitiveTypeInfo) from).getPrimitiveCategory(),
                    ((PrimitiveTypeInfo) to).getPrimitiveCategory());
        }
        return false;
    }

    public static boolean implicitConvertible(PrimitiveObjectInspector.PrimitiveCategory from, PrimitiveObjectInspector.PrimitiveCategory to) {
        if (from == to) {
            return true;
        }

        PrimitiveObjectInspectorUtils.PrimitiveGrouping fromPg = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(from);
        PrimitiveObjectInspectorUtils.PrimitiveGrouping toPg = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(to);

        // Allow implicit String to Double conversion
        if (fromPg == PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP && to == PrimitiveObjectInspector.PrimitiveCategory.DOUBLE) {
            return true;
        }
        // Allow implicit String to Decimal conversion
        if (fromPg == PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP && to == PrimitiveObjectInspector.PrimitiveCategory.DECIMAL) {
            return true;
        }
        // Void can be converted to any type
        if (from == PrimitiveObjectInspector.PrimitiveCategory.VOID) {
            return true;
        }

        // Allow implicit String to Date conversion
        if (fromPg == PrimitiveObjectInspectorUtils.PrimitiveGrouping.DATE_GROUP && toPg == PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP) {
            return true;
        }
        // Allow implicit Numeric to String conversion
        if (fromPg == PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP && toPg == PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP) {
            return true;
        }
        // Allow implicit String to varchar conversion, and vice versa
        if (fromPg == PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP && toPg == PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP) {
            return true;
        }

        // Allow implicit conversion from Byte -> Integer -> Long -> Float -> Double
        // Decimal -> String
        Integer f = numericTypes.get(from);
        Integer t = numericTypes.get(to);
        if (f == null || t == null) {
            return false;
        }
        if (f.intValue() > t.intValue()) {
            return false;
        }
        return true;
    }

    public static void registerNumericType(PrimitiveObjectInspector.PrimitiveCategory primitiveCategory, int level) {
        numericTypeList.add(primitiveCategory);
        numericTypes.put(primitiveCategory, level);
    }

}
