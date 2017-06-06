package cn.whaley.bi.logsys.hive.functions;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.Collector;
import org.junit.Test;

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
}
