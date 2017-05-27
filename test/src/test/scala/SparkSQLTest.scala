import java.sql.DriverManager

import org.junit.Test

class SparkSQLTest {
    @Test
    def testHiveJDBC(): Unit = {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver")
        } catch {
            case ex: Throwable => {
                ex.printStackTrace();
                System.exit(1);
            }
        }
        val con = DriverManager.getConnection("jdbc:hive2://bigdata-appsvr-130-7:10000/metadata", "hive", "hive");
        val stmt = con.createStatement();
        //参数设置测试
        //boolean resHivePropertyTest = stmt.execute("SET tez.runtime.io.sort.mb = 128");

        val sql = "select * from app_id_info";
        System.out.println("Running: " + sql);
        val res = stmt.executeQuery(sql);
        val colCount = res.getMetaData.getColumnCount
        for(i <- 1 to colCount){
            print(res.getMetaData.getColumnLabel(i)+'\t')
        }
        print('\n')
        while (res.next()) {
            for (i <- 1 to colCount) {
                print(res.getObject(i).toString + '\t')
            }
            print('\n')
        }

        res.close()
        con.close()
    }
}