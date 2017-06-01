package cn.whaley.bi.logsys.filecompressor;

/**
 * Created by fj on 17/5/26.
 */


import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;


public class Compressor {

    public static final String CODEC_BZIP2 = "BZip2Codec";
    public static final String CODEC_GZIP = "GzipCodec";
    public static final String CODEC_LZ4 = "Lz4Codec";
    public static final String CODEC_SNAPPY = "SnappyCodec";

    public static final int DEFAULT_BUF_SIZE = 1024 * 1024 * 5;


    /**
     * @param args cmd 指令, compress/decompress
     *             srcPath: 输入路径
     *             outPath: 输出路径
     *             split: 分片数量
     *             bufSize: 缓冲读的字节数
     *             codec: Codec类名, BZip2Codec,GzipCodec,Lz4Codec,SnappyCodec
     *             hadoop.*: hadoop配置项目
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Map<String, String> params = new HashMap<>();
        Configuration conf = new Configuration();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("--")) {
                int idx = arg.indexOf('=');
                String key = arg.substring(2, idx);
                String value = arg.substring(idx + 1);
                if (key.startsWith("hadoop.")) {
                    conf.set(key.substring("hadoop.".length()), value);
                } else {
                    params.put(key, value);
                }
            }
        }

        String cmd = params.get("cmd");
        String srcPath = params.get("srcPath");
        String outPath = params.get("outPath");
        Integer bufSize = DEFAULT_BUF_SIZE;
        Integer split = 1;
        if (params.containsKey("bufSize")) {
            bufSize = Integer.parseInt(params.get("bufSize"));
        }
        if (params.containsKey("split")) {
            split = Integer.parseInt(params.get("split"));
        }
        if (cmd.equalsIgnoreCase("compress")) {
            String codec = "org.apache.hadoop.io.compress." + params.get("codec");
            compress(conf, srcPath, codec, outPath, split, bufSize);
        } else if (cmd.equalsIgnoreCase("decompress"))
            decompress(conf, srcPath, outPath, bufSize);
        else {
            System.err.println("Error!\n usgae: hadoop jar file-compressor.jar [compress] [filename] [compress type]");
            System.err.println("\t\ror [decompress] [filename] ");
            return;
        }
        System.out.println("down");
    }


    /**
     * 压缩
     *
     * @param filePath 需要压缩的源文件路径
     * @param codecCls 压缩类
     * @throws ClassNotFoundException
     * @throws IOException
     */
    public static void compress(Configuration conf, String filePath, String codecCls, String targetPath, Integer split, Integer bufSize) throws ClassNotFoundException, IOException {
        System.out.println("[" + new Date() + "] : enter compress");

        FileSystem fs = FileSystem.get(conf);

        Path pathIn = new Path(filePath);
        InputStream in;
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec srcCodec = factory.getCodec(pathIn);
        if (null != srcCodec) {
            in = srcCodec.createInputStream(fs.open(pathIn));
        } else {
            in = fs.open(pathIn);
        }

        Class codecClass = Class.forName(codecCls);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);

        if (targetPath == null || targetPath == "") {
            targetPath = filePath;
        }
        if (!targetPath.endsWith(codec.getDefaultExtension())) {
            targetPath = targetPath + codec.getDefaultExtension();
        }

        System.out.println("[" + new Date() + "]: start compressing,split = " + split);

        if (split > 1) {
            CompressionOutputStream[] outs = new CompressionOutputStream[split];
            String prefix = targetPath.substring(0, targetPath.lastIndexOf('.'));
            String ext = targetPath.substring(targetPath.lastIndexOf('.') + 1);
            for (int i = 0; i < split; i++) {
                String path = prefix + "." + i + "." + ext;
                outs[i] = codec.createOutputStream(fs.create(new Path(path), true));
                System.out.println("[" + new Date() + "]: target file " + path);
            }
            copyBytes(in, outs, bufSize, true);
        } else {
            String path = targetPath;
            if (targetPath.endsWith(filePath)) {
                String prefix = targetPath.substring(0, targetPath.lastIndexOf('.'));
                String ext = targetPath.substring(targetPath.lastIndexOf('.') + 1);
                path = prefix + ".1." + ext;
            }
            System.out.println("[" + new Date() + "]: target file " + path);
            CompressionOutputStream out = codec.createOutputStream(fs.create(new Path(path), true));
            copyBytes(in, out, bufSize, true);
        }

        System.out.println("[" + new Date() + "]: compressing finished ");

    }


    /**
     * 解压
     *
     * @param filePath 需要解压的源文件路径
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static void decompress(Configuration conf, String filePath, String targetPath, Integer bufSize) throws FileNotFoundException, IOException {
        System.out.println("[" + new Date() + "] : enter decompress");

        FileSystem fs = FileSystem.get(conf);

        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(new Path(filePath));
        if (null == codec) {
            System.out.println("Cannot find codec for file " + filePath);
            return;
        }

        Path inFilePath = new Path(filePath);
        Path outFilePath;
        if (targetPath != null && targetPath != "") {
            outFilePath = new Path(targetPath);
        } else {
            outFilePath = new Path(CompressionCodecFactory.removeSuffix(filePath, codec.getDefaultExtension()));
        }

        InputStream in = codec.createInputStream(fs.open(inFilePath));
        OutputStream out = fs.create(outFilePath, true);

        System.out.println("[" + new Date() + "]: start decompressing ");
        copyBytes(in, out, bufSize, true);
        System.out.println("[" + new Date() + "]: decompressing finished ");

    }

    public static void copyBytes(InputStream in, OutputStream out, int buffSize, boolean close)
            throws IOException {
        try {
            copyBytes(in, out, buffSize);
            if (close) {
                out.close();
                out = null;
                in.close();
                in = null;
            }
        } finally {
            if (close) {
                IOUtils.closeStream(out);
                IOUtils.closeStream(in);
            }
        }
    }

    public static void copyBytes(InputStream in, OutputStream[] outs, int buffSize, boolean close)
            throws IOException {
        try {
            copyBytes(in, outs, buffSize);
        } finally {
            if (close) {
                for (int i = 0; i < outs.length; i++) {
                    OutputStream out = outs[i];
                    IOUtils.closeStream(out);
                }
                IOUtils.closeStream(in);
            }
        }
    }

    public static void copyBytes(InputStream in, OutputStream out, int buffSize)
            throws IOException {
        PrintStream ps = out instanceof PrintStream ? (PrintStream) out : null;
        byte buf[] = new byte[buffSize];
        long taskSize = 0;
        long totalSize = 0;
        int bytesRead = in.read(buf);
        while (bytesRead >= 0) {
            taskSize += bytesRead;
            totalSize += bytesRead;
            out.write(buf, 0, bytesRead);
            if ((ps != null) && ps.checkError()) {
                throw new IOException("Unable to write to output stream.");
            }
            if (taskSize >= 1024 * 1024 * 100) {
                taskSize = 0;
                System.out.println("[" + new Date() + "]: total transferred bytes : " + totalSize);
            }
            bytesRead = in.read(buf);
        }
    }

    public static void copyBytes(InputStream in, OutputStream[] outs, int buffSize)
            throws IOException {
        long ts = System.currentTimeMillis();

        int splitCount = outs.length;
        ExecutorService service = Executors.newFixedThreadPool(outs.length);

        final byte zero[] = new byte[0];
        byte buf[] = new byte[buffSize];
        byte bufRemain[] = zero;
        long[] taskSize = new long[splitCount];
        long[] totalSize = new long[splitCount];

        int bytesRead = in.read(buf);
        while (bytesRead >= 0) {

            //避免单行数据被分割到不同的文件中,最后一个换行符的数据累积到下一个数据块中
            int bufSplitAt = bytesRead - 1;
            for (int k = bytesRead - 1; k > 0; k--) {
                if (buf[k] == '\n') {
                    bufSplitAt = k;
                }
            }

            //合并前一个块遗留的数据
            byte outBytes[];
            if (bufRemain.length > 0) {
                outBytes = new byte[bufRemain.length + bufSplitAt + 1];
                System.arraycopy(bufRemain, 0, outBytes, 0, bufRemain.length);
                System.arraycopy(buf, 0, outBytes, bufRemain.length, bufSplitAt + 1);
            } else {
                outBytes = new byte[bufSplitAt + 1];
                System.arraycopy(buf, 0, outBytes, 0, bufSplitAt + 1);
            }

            //本块遗留的数据
            if (bufSplitAt == bytesRead - 1) {
                bufRemain = zero;
            } else {
                bufRemain = Arrays.copyOfRange(buf, bufSplitAt + 1, bytesRead);
            }

            //分片
            List<TaskCallable> tasks = new ArrayList<>();
            List<Integer> outSplitAt = getSplitIndex(outBytes, splitCount);
            for (int i = 0; i < outSplitAt.size(); i++) {

                int id = i;
                int from = 0;
                if (i > 0) {
                    from = outSplitAt.get(i - 1) + 1;
                }
                int to = outSplitAt.get(i) + 1;
                TaskCallable task = new TaskCallable(id, outs[id], outBytes, from, to, taskSize, totalSize);
                tasks.add(task);

                //末尾分片
                if (i == outSplitAt.size() - 1) {
                    int lastSplitAt = outSplitAt.get(i);
                    if (lastSplitAt < outBytes.length - 1) {
                        from = lastSplitAt + 1;
                        to = outBytes.length;
                        id = i + 1;
                        task = new TaskCallable(id, outs[id], outBytes, from, to, taskSize, totalSize);
                        tasks.add(task);
                    }
                }
            }
            try {
                List<Future<Integer>> futures = service.invokeAll(tasks);
                for (int i = 0; i < futures.size(); i++) {
                    Integer result = futures.get(i).get();
                    if (result < 0) {
                        throw new RuntimeException("task " + i + " result: " + result);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }

            //读取下一个块
            bytesRead = in.read(buf);
        }

        service.shutdown();

        long total = 0;
        for (int i = 0; i < totalSize.length; i++) {
            total += totalSize[i];
        }
        System.out.println("[" + new Date() + "]: task completed, total size " + total + ", ts " + (System.currentTimeMillis() - ts));

    }

    private static List<Integer> getSplitIndex(byte[] bytes, int splitCount) {
        long len = bytes.length;
        List<Integer> indexes = new ArrayList<>();
        long splitSize = len / splitCount;
        long currSize = 0;
        for (int i = 0; i < len; i++) {
            currSize++;
            if (bytes[i] == '\n' && currSize >= splitSize) {
                indexes.add(i);
                currSize = 0;
            }
        }
        return indexes;
    }

    private static class TaskCallable implements Callable<Integer> {

        private int id;
        private long[] taskSize;
        private long[] totalSize;
        private OutputStream out;
        private byte[] outBytes;
        private int from;
        private int to;

        public TaskCallable(int id, OutputStream out, byte[] outBytes, int from, int to, long[] taskSize, long[] totalSize) {
            this.taskSize = taskSize;
            this.totalSize = totalSize;
            this.out = out;
            this.id = id;
            this.outBytes = outBytes;
            this.from = from;
            this.to = to;
        }

        @Override
        public Integer call() throws Exception {
            PrintStream ps = out instanceof PrintStream ? (PrintStream) out : null;
            int taskBytes = to - from;
            taskSize[id] += taskBytes;
            totalSize[id] += taskBytes;
            try {
                out.write(outBytes, from, to - from);
                if ((ps != null) && ps.checkError()) {
                    throw new IOException("Unable to write to output stream.");
                }
                if (taskSize[id] >= 1024 * 1024 * 100) {
                    taskSize[id] = 0;
                    System.out.println("[" + new Date() + "]: id=" + id + ", total transferred bytes : " + totalSize[id]);
                }
                return 1;
            } catch (IOException e) {
                System.out.println("[" + new Date() + "]: from=" + from + ",to=" + to + ",outBytes.length=" + outBytes.length);
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }
}