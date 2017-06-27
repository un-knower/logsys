package cn.whaley.bi.logsys.hive.functions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.Collector;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;


/**
 * Created by fj on 17/6/6.
 */
public class JsonArrayStrExplodeTest {
    @Test
    public void test1() throws HiveException {
        JsonArrayStrExplode func = new JsonArrayStrExplode();
        func.setCollector(new Collector() {
            @Override
            public void collect(Object o) throws HiveException {
                Object[] vs = (Object[]) o;
                System.out.println(String.format("%s,%s", vs[0], vs[1]));
            }
        });
        Object[] args = new Object[]{"[{\"rowId\":\"1\"},{\"rowId\":\"2\"}]"};
        func.process(args);
    }

    @Test
    public void test2(){
        Configuration conf= new Configuration();

        for(Iterator<Map.Entry<String, String>> it=conf.iterator();it.hasNext();){
            Map.Entry<String, String> entry=it.next();
            System.out.println(entry.getKey()+"="+entry.getValue());
        }
        //conf.getStrings()
    }
}
