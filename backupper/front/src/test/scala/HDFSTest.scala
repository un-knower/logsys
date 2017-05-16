import java.io.ByteArrayOutputStream

import com.alibaba.fastjson.JSONObject
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.IOUtils
import org.junit.Test

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
 * Created by fj on 17/4/21.
 */
class HDFSTest {

    /*

drop table if exists test.json_test2;
CREATE TABLE test.json_test2(
    id string,name string,src string
)
PARTITIONED BY ( key_time int)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
WITH SERDEPROPERTIES (
  "serialization.row.decode.require" = "false"
  ,"serialization.row.decode.allow_unquoted_backslash" = "false"
)
STORED AS TEXTFILE;

alter table test.json_test2 add if not exists partition(key_time=0);

select * from test.json_test2 a;

insert overwrite table test.json_test2 partition(key_time=0)
    select id,name,src from test.json_test2 a;

     */


    @Test
    def testHDFSList(): Unit = {
        val files = HDFSTest.getFS().listFiles(new Path("/user/hive/warehouse/test.db"), false)
        while (files.hasNext) {
            val cur = files.next()
            println(s"${cur.getPath}");
        }

    }

    @Test
    def testHDFSWrite(): Unit = {
        val path = new Path("/user/hive/warehouse/test.db/json_test2/key_time=0/curr.json");

        val fs = HDFSTest.getFS();
        fs.createNewFile(path)

        //HDFSTest.getFS().setTimes()
        var stream = fs.append(path);

        var i = 0;
        val count = 10000;
        while (i < count) {
            val obj: JSONObject = new JSONObject
            val srcObj: JSONObject = new JSONObject
            srcObj.put("ip", "127.0.0.1")
            srcObj.put("host", "localhost")
            val id: Long = System.currentTimeMillis
            obj.put("id", id)
            obj.put("name", "name_" + id)
            obj.put("src", srcObj)

            val jsonStr = obj.toJSONString;
            stream.writeBytes(jsonStr)
            stream.writeByte('\n')
            i = i + 1

            //stream.hflush();
            //Thread.sleep(1000)
            //println(s"${i} : ${jsonStr}")

            /*
            if (i % 10 == 0) {
                //理论上hflush后其他新创建的文件read能读取文件内容
                //但实际测试发现,只有stream重新打开其他read才能确保读到新追加的文件内容
                //因为hflush之前创建的read还是不能读到新文件内容
                stream.close();
                stream = HDFSTest.getFS().append(path);
                println(s"${i} reopen.")
            }
            */
        }
        stream.close();
    }


    @Test
    def testHDFSTail(): Unit = {
        val path = new Path("/user/hive/warehouse/test.db/json_test2/key_time=0/curr.json");
        val fs = HDFSTest.getFS();
        //HDFSTest.dumpFromOffset(fs, path, -2048)
        val str = HDFSTest.getFromOffset(fs, path, -2048);
        System.out.println(str);
    }

    @Test
    def testHDFSWrite2(): Unit = {
        val path = new Path("/tmp/1.txt");
        for(i <- 1 to 10){
            val fs = HDFSTest.getFS();
            fs.createNewFile(path)
            val out=fs.append(path)
            out.write("test".getBytes)
            IOUtils.closeStream(out);
            fs.close()
            println(s"write ${i}")
        }
    }



}

object HDFSTest {
    def getFS(): FileSystem = {
        val conf = new Configuration()
        FileSystem.get(conf)
    }

    def dumpFromOffset(fs: FileSystem, path: Path, startOffset: Long): Long = {
        var offset = startOffset
        val fileSize = fs.getFileStatus(path).getLen;
        if (offset > fileSize) return fileSize;
        // treat a negative offset as relative to end of the file, floor of 0
        if (offset < 0) {
            offset = Math.max(fileSize + offset, 0);
        }

        val in = fs.open(path);
        try {
            in.seek(offset);
            // use conf so the system configured io block size is used
            IOUtils.copyBytes(in, System.out, fs.getConf(), false);
            offset = in.getPos();
        } finally {
            in.close();
        }
        offset;
    }

    def getFromOffset(fs: FileSystem, path: Path, startOffset: Long): String = {
        var offset = startOffset
        val fileSize = fs.getFileStatus(path).getLen;
        if (offset > fileSize) return "";
        // treat a negative offset as relative to end of the file, floor of 0
        if (offset < 0) {
            offset = Math.max(fileSize + offset, 0);
        }

        val in = fs.open(path);
        try {
            in.seek(offset);
            // use conf so the system configured io block size is used
            val buf = new ByteArrayOutputStream();
            IOUtils.copyBytes(in, buf, fs.getConf(), false);
            new String(buf.toByteArray);
        } finally {
            in.close();
        }
    }


}