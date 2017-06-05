package cn.whaley.bi.logsys.hive.functions;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Created by fj on 17/6/5.
 */
public class CreateAppIdTest {
    /*
boikgpokn78sb95ktmsc1bnkechpgj9l	whaley	medusa	main3.1
boikgpokn78sb95ktmsc1bnken8tuboa	whaley	medusa	main2.0
boikgpokn78sb95ktmsc1bnkbe9pbhgu	whaley	medusa	main1.0
boikgpokn78sb95ktmsc1bnkfipphckl	whaley	medusa	kids
boikgpokn78sb95kjtihcg268dc5mlsr	whaley	mobilehelper	main
boikgpokn78sb95kjhfrendo8dc5mlsr	whaley	whaleytv	main
boikgpokn78sb95kbqei6cc98dc5mlsr	whaley	whaleyvr	main
boikgpokn78sb95kkls3bhmtjqosocdj	whaley	crawler	price
boikgpokn78sb95kjhfrendobgjgjolq	whaley	whaleytv	epop
boikgpokn78sb95kjhfrendoepkseljn	whaley	whaleytv	global_menu_2.0
boikgpokn78sb95ktmsc1bnkklf477ap	whaley	medusa	main3.0
boikgpokn78sb95kjhfrendoj8ilnoi7	whaley	whaleytv	wui2.0
boikgpokn78sb95kicggqhbkepkseljn	whaley	orca	global_menu_2.0
     */

    @Test
    public void Test1() throws HiveException {
        Object v = getCreateAppIdResult("whaley", "medusa", "main1.0");
        System.out.println(String.join("\t", v.toString(), "whaley", "medusa", "main1.0"));
        Assert.assertEquals(v, "boikgpokn78sb95ktmsc1bnkbe9pbhgu");

        v = getCreateAppIdResult("whaley", "medusa", "main2.0");
        System.out.println(String.join("\t", v.toString(), "whaley", "medusa", "main2.0"));
        Assert.assertEquals(v, "boikgpokn78sb95ktmsc1bnken8tuboa");

        v = getCreateAppIdResult("whaley", "whaleytv", "global_menu_2.0");
        System.out.println(String.join("\t", v.toString(), "whaley", "whaleytv", "global_menu_2.0"));
        Assert.assertEquals(v, "boikgpokn78sb95kjhfrendoepkseljn");

    }

    @Test
    public void Test2() throws HiveException {
        Object v = getCreateAppIdResult2("whaley", "medusa", "main1.0", "", "", "");
        System.out.println(String.join("\t", v.toString(), "whaley", "medusa", "main1.0"));
        Assert.assertEquals(v, "boikgpokn78sb95ktmsc1bnkbe9pbhgu");

        v = getCreateAppIdResult2("whaley", "medusa", "main2.0", "", "", "");
        System.out.println(String.join("\t", v.toString(), "whaley", "medusa", "main2.0"));
        Assert.assertEquals(v, "boikgpokn78sb95ktmsc1bnken8tuboa");

        v = getCreateAppIdResult2("whaley", "whaleytv", "global_menu_2.0", "", "", "");
        System.out.println(String.join("\t", v.toString(), "whaley", "whaleytv", "global_menu_2.0"));
        Assert.assertEquals(v, "boikgpokn78sb95kjhfrendoepkseljn");

    }

    private Object getCreateAppIdResult(String orgCode, String productCode, String appCode) throws HiveException {
        ObjectInspector[] inputOIs = {
                PrimitiveObjectInspectorFactory.writableStringObjectInspector
                , PrimitiveObjectInspectorFactory.writableStringObjectInspector
                , PrimitiveObjectInspectorFactory.writableStringObjectInspector
        };

        GenericUDF.DeferredJavaObject orgCodeObj = new GenericUDF.DeferredJavaObject(orgCode);
        GenericUDF.DeferredJavaObject productCodeObj = new GenericUDF.DeferredJavaObject(productCode);
        GenericUDF.DeferredJavaObject appCodeObj = new GenericUDF.DeferredJavaObject(appCode);
        GenericUDF.DeferredObject[] arguments = new GenericUDF.DeferredObject[3];
        arguments[0] = orgCodeObj;
        arguments[1] = productCodeObj;
        arguments[2] = appCodeObj;

        CreateAppId fn = getCreateAppIdFn(inputOIs);
        Object v = fn.evaluate(arguments);
        return v;
    }

    private Object getCreateAppIdResult2(String orgCode, String productCode, String appCode, String orgCodeNoise, String productCodeNoise, String appCodeNoise) throws HiveException {
        PrimitiveTypeInfo typeInfo = (PrimitiveTypeInfo) TypeInfoFactory.getPrimitiveTypeInfoFromJavaPrimitive(String.class);
        ObjectInspector[] inputOIs = {
                PrimitiveObjectInspectorFactory.writableStringObjectInspector
                , PrimitiveObjectInspectorFactory.writableStringObjectInspector
                , PrimitiveObjectInspectorFactory.writableStringObjectInspector
                , new JavaConstantStringObjectInspector(orgCodeNoise)
                , new JavaConstantStringObjectInspector(productCodeNoise)
                , new JavaConstantStringObjectInspector(appCodeNoise)
        };

        GenericUDF.DeferredJavaObject orgCodeObj = new GenericUDF.DeferredJavaObject(orgCode);
        GenericUDF.DeferredJavaObject productCodeObj = new GenericUDF.DeferredJavaObject(productCode);
        GenericUDF.DeferredJavaObject appCodeObj = new GenericUDF.DeferredJavaObject(appCode);
        GenericUDF.DeferredJavaObject orgCodeNoiseObj = new GenericUDF.DeferredJavaObject(orgCodeNoise);
        GenericUDF.DeferredJavaObject productCodeNoiseObj = new GenericUDF.DeferredJavaObject(productCodeNoise);
        GenericUDF.DeferredJavaObject appCodeNoiseObj = new GenericUDF.DeferredJavaObject(appCodeNoise);
        GenericUDF.DeferredObject[] arguments = new GenericUDF.DeferredObject[6];
        arguments[0] = orgCodeObj;
        arguments[1] = productCodeObj;
        arguments[2] = appCodeObj;
        arguments[3] = orgCodeNoiseObj;
        arguments[4] = productCodeNoiseObj;
        arguments[5] = appCodeNoiseObj;

        CreateAppId fn = getCreateAppIdFn(inputOIs);
        Object v = fn.evaluate(arguments);
        return v;
    }

    private CreateAppId getCreateAppIdFn(ObjectInspector[] inputOIs) throws UDFArgumentException {

        CreateAppId fn = new CreateAppId();
        fn.initialize(inputOIs);
        return fn;
    }
}
