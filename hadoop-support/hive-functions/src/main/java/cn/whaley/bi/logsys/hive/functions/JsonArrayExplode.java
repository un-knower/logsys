package cn.whaley.bi.logsys.hive.functions;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;

/**
 * Created by fj on 17/6/6.
 */
@Description(name = "_FUNC_"
        , value = "_FUNC_(jsonArrayStr:String,colNames:String)"
        , extended = "version:" + PackageConstants.FN_VERSION + "." + JsonArrayExplode.BUILD_ID + ", out fields: row_num:int,row_value:string, example: SELECT _FUNC_('[{\"name\":\"name1\"},{\"name\":\"name2\"}]');"
)
public class JsonArrayExplode extends GenericUDTF {
    private static final Logger LOG = LoggerFactory.getLogger(JsonArrayExplode.class);
    public final static String BUILD_ID = "1";

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        if (argOIs.getAllStructFieldRefs().size() != 2) {
            throw new UDFArgumentException("required 2 parameters.");
        }
        //第2个参数必须为常量
        ObjectInspector oi = argOIs.getAllStructFieldRefs().get(1).getFieldObjectInspector();
        if (!(oi instanceof ConstantObjectInspector)) {
            throw new UDFArgumentTypeException(1, 1 + "  must be a constant value, " + oi.getTypeName() + " was passed.");
        }

        jsonFactory = new JsonFactory();
        jsonFactory.enable(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS);

        String colNamesStr = ((ConstantObjectInspector) oi).getWritableConstantValue().toString();
        String[] colNamesArray = colNamesStr.split(",");
        fieldNames = new ArrayList<>();
        fieldNames.add("row_num");
        for (int i = 0; i < colNamesArray.length; i++) {
            if (StringUtils.isNotEmpty(colNamesArray[i])) {
                fieldNames.add(colNamesArray[i].trim());
            }
        }
        ArrayList<ObjectInspector> inputOIs = new ArrayList<ObjectInspector>();
        inputOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        for (int i = 1; i < fieldNames.size(); i++) {
            inputOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }
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
                    String objStr = jsonArrayStr.substring(prePosition, i + 1);
                    List<String> fieldValues = parse(objStr, fieldNames);
                    Object[] rowData = new Object[fieldNames.size() + 1];
                    rowData[0] = rowNum++;
                    for (int k = 1; k < rowData.length; i++) {
                        rowData[k] = fieldValues.get(k);
                    }
                    forward(rowData);
                }
            }
        }
    }

    @Override
    public void close() throws HiveException {
    }

    private List<String> fieldNames;

    private JsonFactory jsonFactory = null;

    private List<String> parse(String jsonObjStr, List<String> columnNames) {

        JsonParser p;
        List<String> r = new ArrayList<String>(Collections.nCopies(columnNames.size(), null));
        try {
            byte[] bytes = jsonObjStr.getBytes();

            p = jsonFactory.createJsonParser(new ByteArrayInputStream(bytes));
            if (p.nextToken() != JsonToken.START_OBJECT) {
                throw new IOException("Start token not found where expected");
            }
            JsonToken token;
            while (((token = p.nextToken()) != JsonToken.END_OBJECT) && (token != null)) {
                populateRecord(r, token, p, columnNames);
            }
            return r;
        } catch (JsonParseException e) {
            LOG.warn("Error [{}] parsing json text [{}].", e, jsonObjStr);
            LOG.debug(null, e);
            return r;
        } catch (IOException e) {
            LOG.warn("Error [{}] parsing json text [{}].", e, jsonObjStr);
            LOG.debug(null, e);
            return r;
        }
    }

    private void populateRecord(List<String> r, JsonToken token, JsonParser p, List<String> columnNames) throws IOException {
        if (token != JsonToken.FIELD_NAME) {
            throw new IOException("Field name expected");
        }
        String fieldName = p.getText();
        Integer fpos = columnNames.indexOf(fieldName);
        if (fpos == null || fpos < 0) {
            skipValue(p);
            return;
        }
        String currField = extractCurrentField(p, false);
        r.set(fpos, currField);
    }

    private String extractCurrentField(JsonParser p, boolean isTokenCurrent) throws IOException {
        String val = null;
        JsonToken valueToken;
        if (isTokenCurrent) {
            valueToken = p.getCurrentToken();
        } else {
            valueToken = p.nextToken();
        }

        if (valueToken == JsonToken.VALUE_NULL) {
            val = null;
        } else {
            //Map类型
            if (valueToken == JsonToken.START_OBJECT) {
                StringBuilder strVal = new StringBuilder();
                strVal.append("{");
                while ((valueToken = p.nextToken()) != JsonToken.END_OBJECT) {
                    String k = p.getCurrentName().toString();
                    String v = (String) extractCurrentField(p, false);
                    if (strVal.length() > 1) {
                        strVal.append(",");
                    }
                    if (k == null) {
                        k = "null";
                    } else {
                        k = "\"" + k + "\"";
                    }
                    if (v == null) {
                        v = "null";
                    } else {
                        if (!(v.startsWith("{") && v.endsWith("}"))) {
                            v = "\"" + v + "\"";
                        }
                    }
                    strVal.append(k + ":" + v);
                }
                strVal.append("}");
                val = strVal.toString();
            } else if (valueToken == JsonToken.START_ARRAY) {
                StringBuilder strVal = new StringBuilder();
                strVal.append("[");
                while ((valueToken = p.nextToken()) != JsonToken.END_ARRAY) {
                    String v = (String) extractCurrentField(p, true);
                    if (strVal.length() > 1) {
                        strVal.append(',');
                    }
                    if (v == null) {
                        v = "null";
                    } else {
                        if (!(v.startsWith("{") && v.endsWith("}"))) {
                            v = "\"" + v + "\"";
                        }
                    }
                    strVal.append(v);
                }
                strVal.append("]");
                val = strVal.toString();
            } else {
                val = p.getText();
            }
        }
        return val;
    }

    private void skipValue(JsonParser p) throws JsonParseException, IOException {
        JsonToken valueToken = p.nextToken();
        if ((valueToken == JsonToken.START_ARRAY) || (valueToken == JsonToken.START_OBJECT)) {
            // if the currently read token is a beginning of an array or object, move stream forward
            // skipping any child tokens till we're at the corresponding END_ARRAY or END_OBJECT token
            p.skipChildren();
        }
        // At the end of this function, the stream should be pointing to the last token that
        // corresponds to the value being skipped. This way, the next call to nextToken
        // will advance it to the next field name.
    }

}

