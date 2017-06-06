package cn.whaley.bi.logsys.hive.functions;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.*;

/**
 * Created by fj on 17/6/6.
 */
@Description(name = "JsonArrayStrExplode"
        , value = "JsonArrayStrExplode(jsonArrayStr:String)"
        , extended = "version:" + PackageConstants.FN_VERSION + ", out fields: row_num:int,row_value:string, example: SELECT JsonArrayStrExplode('[{\"name\":\"name1\"},{\"name\":\"name2\"}]');"
)
public class JsonArrayStrExplode extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        List inputFields = argOIs.getAllStructFieldRefs();

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> inputOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("row_num");
        fieldNames.add("row_value");
        inputOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        inputOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, inputOIs);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        if (objects[0] == null) {
            return;
        }

        String jsonArrayStr = objects[0].toString();
        if (!jsonArrayStr.startsWith("[") || !jsonArrayStr.endsWith("]")) {
            throw new HiveException("invalid data:" + jsonArrayStr);
        }

        Integer rowNum = 0;
        Integer prePosition = 0;
        Stack<String> bracket = new Stack<>();
        for (Integer i = 0; i < jsonArrayStr.length(); i++) {
            char chr = jsonArrayStr.charAt(i);
            if (chr == '{') {
                if (bracket.empty()) {
                    prePosition = i;
                }
                bracket.push("{");
            } else if (chr == '}') {
                bracket.pop();
                if (bracket.empty()) {
                    rowNum++;
                    String objStr = jsonArrayStr.substring(prePosition, i + 1);
                    forward(new Object[]{rowNum++, objStr});
                }
            }
        }
    }

    @Override
    public void close() throws HiveException {
    }

}

