/**
 * Created by fj on 17/4/26.
 */

import java.io.IOException;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

/*
drop table if exists test.json_test1;
CREATE TABLE test.json_test1(
    id string,name string,src string
)
PARTITIONED BY ( key_time int)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
WITH SERDEPROPERTIES (
  "serialization.row.decode.require" = "false"
  ,"serialization.row.decode.allow_unquoted_backslash" = "false"
)
STORED AS sequencefile;

alter table test.json_test1 add if not exists partition(key_time=0);

truncate table test.json_test1 ;
select * from test.json_test1 a;

insert overwrite table test.json_test1 partition(key_time=0)
  select 'id_0','name_0','src_0';

insert overwrite table test.json_test1 partition(key_time=1)
  select id,name,src from test.json_test1
  where key_time=0;

dfs -ls /user/hive/warehouse/test.db/json_test1/key_time=0;

 */
public class HDFSSequnceFileTest {

    @Test
    public void testRead() throws IOException {
        String uri = "/user/hive/warehouse/test.db/json_test1/key_time=0/0.seq";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(uri);

        SequenceFile.Reader reader = null;
        try {
            //返回 SequenceFile.Reader 对象
            reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
            Text lastValue = reader.getMetadata().get(new Text("lastValue"));
            if (lastValue == null) {
                System.out.println("lastValue is null");
            } else {
                System.out.println("lastValue is " + lastValue);
            }

            //reader.seek(5561);
            //getKeyClass()获得Sequence中使用的类型
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            //ReflectionUtils.newInstace 得到常见的键值的实例
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);//同上
            long position = reader.getPosition();
            int i = 0;
            while (reader.next(key, value)) { //next（）方法迭代读取记录 直到读完返回false
                String syncSeen = reader.syncSeen() ? "*" : "";//替换特殊字符 同步
                System.out.printf("%s [%s%s]\t%s\t%s\n", (++i), position, syncSeen, key, value);
                position = reader.getPosition(); // beginning of next record
                reader.getMetadata().set(new Text("lastValue"), (Text) value);
            }
        } finally {
            IOUtils.closeStream(reader);
        }
    }

    @Test
    public void testWrite() throws IOException, InterruptedException {

        //String uri = "/user/hive/warehouse/test.db/json_test1/key_time=0/" + System.currentTimeMillis() + ".seq";
        String uri = "/user/hive/warehouse/test.db/json_test1/key_time=0/0.seq";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(uri);

        SequenceFile.Writer writer = null;
        try {
            writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(path)
                    , SequenceFile.Writer.keyClass(Text.class),
                    SequenceFile.Writer.valueClass(Text.class),
                    SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK)
            );

            Integer i = 0;
            Integer count = 3;
            while (i < count) {
                JSONObject obj = new JSONObject();
                JSONObject srcObj = new JSONObject();
                srcObj.put("ip", "127.0.0.1");
                srcObj.put("host", "localhost");
                Long id = System.currentTimeMillis();
                obj.put("id", id);
                obj.put("name", "name_" + id);
                obj.put("src", srcObj);
                writer.append(new Text(id.toString()), new Text(obj.toJSONString()));
                //Thread.sleep(3000);

                i++;
                /*
                if (i % 100 == 0) {
                    writer.hsync();
                    System.out.println("write count: " + i);
                }
                */
            }

        } finally {
            IOUtils.closeStream(writer);
        }
    }

}

