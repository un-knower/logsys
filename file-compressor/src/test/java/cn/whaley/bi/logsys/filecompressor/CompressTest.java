package cn.whaley.bi.logsys.filecompressor;

import org.junit.Test;

import java.io.IOException;
import java.net.URL;

/**
 * Created by fj on 17/5/26.
 */
public class CompressTest {
    @Test
    public void testCompress() throws InterruptedException, IOException, ClassNotFoundException {
        URL resURL = this.getClass().getClassLoader().getResource("./data/moretv.com.cn.post.txt");
        System.out.println(resURL.getPath());
        String filePath = "file://" + resURL.getPath();
        String[] args = new String[]{"compress",filePath,Compressor.CODEC_BZIP2};
        Compressor.main(args);
    }
    @Test
    public void testDecompress() throws InterruptedException, IOException, ClassNotFoundException {
        URL resURL = this.getClass().getClassLoader().getResource("./data/moretv.com.cn.post.txt.bz2");
        System.out.println(resURL.getPath());
        String filePath = "file://" + resURL.getPath();
        String[] args = new String[]{"decompress",filePath};
        Compressor.main(args);
    }
}
