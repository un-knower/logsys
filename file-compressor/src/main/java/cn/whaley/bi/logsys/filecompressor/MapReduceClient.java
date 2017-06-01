package cn.whaley.bi.logsys.filecompressor;

/**
 * Created by fj on 17/5/31.
 */


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.*;


import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MapReduceClient {

    public static final String DEFAULT_WORK_DIR = "/tmp/compressor";
    public static final String CONF_KEY_TASK_CMD_PARAMS = "fileCompressor.taskCmdParams";
    public static final Logger LOG = LoggerFactory.getLogger(MapReduceClient.class);


    static class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            JSONObject params = JSON.parseObject(conf.get(CONF_KEY_TASK_CMD_PARAMS));
            String cmd = params.getString("cmd");
            String codec = params.getString("codec");
            Integer bufSize = params.getInteger("bufSize");
            Integer split = params.getInteger("split");

            LOG.info("params:" + params.toJSONString());

            String[] line = value.toString().split("\t");
            String srcFilePath = line[0];
            String targetFilePath = line[1];

            if (cmd.equalsIgnoreCase("compress")) {
                try {
                    JSONObject out = new JSONObject();
                    out.put("codec", codec);
                    out.put("targetFilePath", targetFilePath);
                    out.put("split", split);
                    Long ts = System.currentTimeMillis();
                    Compressor.compress(conf, srcFilePath, codec, targetFilePath, split, bufSize);
                    out.put("ts", System.currentTimeMillis() - ts);
                    context.write(new Text(srcFilePath), new Text(out.toJSONString()));
                } catch (ClassNotFoundException e) {
                    throw new IOException(e);
                }
            } else if (cmd.equalsIgnoreCase("decompress")) {
                try {
                    Compressor.decompress(conf, srcFilePath, targetFilePath, bufSize);
                } catch (Exception e) {
                    throw new IOException(e);
                }
            } else {
                throw new IOException("invalid cmd:" + cmd);
            }
        }

    }


    static class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }

    }


    /**
     * args: [genericOptions] [commandOptions]
     * <p/>
     * genericOptions:
     * <p/>
     * -conf <configuration file>     specify a configuration file
     * -D <property=value>            use value for given property
     * -fs <local|namenode:port>      specify a namenode
     * -jt <local|jobtracker:port>    specify a job tracker
     * -files <comma separated list of files>    specify comma separated
     * files to be copied to the map reduce cluster
     * -libjars <comma separated list of jars>   specify comma separated
     * jar files to include in the classpath.
     * -archives <comma separated list of archives>    specify comma
     * separated archives to be unarchived on the compute machines.
     * <p/>
     * commandOptions:
     * <p/>
     * --cmd 指令, compress/decompress
     * --workDir: job工作路径,任务文件将在该目录产生,默认为/tmp/compressor
     * --srcPath: 输入路径
     * --outPath: 输出路径
     * --split: 分片数量
     * --bufSize: 缓冲读的字节数
     * --codec: Codec类名, BZip2Codec,GzipCodec,Lz4Codec,SnappyCodec
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        LOG.info("cmd args:" + String.join(" , ", Arrays.asList(args)));
        LOG.info("other args:" + String.join(" , ", Arrays.asList(otherArgs)));

        JSONObject params = new JSONObject();
        params.put("workDir", DEFAULT_WORK_DIR);
        params.put("bufSize", Compressor.DEFAULT_BUF_SIZE);
        params.put("split", 1);
        for (int i = 0; i < otherArgs.length; i++) {
            String arg = otherArgs[i];
            if (arg.startsWith("--")) {
                Integer idx = arg.indexOf('=');
                String key = arg.substring(2, idx);
                String value = arg.substring(idx + 1);
                if(key.equalsIgnoreCase("codec")){
                    if(value.indexOf('.')<0){
                        value="org.apache.hadoop.io.compress." +value;
                    }
                }
                params.put(key, value);
            }
        }
        conf.set(CONF_KEY_TASK_CMD_PARAMS, params.toJSONString());
        LOG.info("cmd params:" + params.toJSONString());


        String cmd = params.getString("cmd");
        String workDir = params.getString("workDir");
        String srcPath = params.getString("srcPath");
        String outPath = params.getString("outPath");
        String taskId = new SimpleDateFormat("yyyyMMdd_hhmmss_SSS").format(new Date());
        String taskIn = workDir + "/" + cmd + "_" + taskId + ".list";
        String taskOut = workDir + "/" + cmd + "_" + taskId;

        LOG.info("taskIn:"+taskIn);
        LOG.info("taskOut:"+taskOut);

        //产生任务文件
        FileSystem fs = FileSystem.get(conf);
        List<Path> targetFiles = new ArrayList<>();
        FileStatus[] statuses = fs.globStatus(new Path(srcPath));
        for (Integer i = 0; i < statuses.length; i++) {
            FileStatus status = statuses[i];
            if (status.isFile() && status.getLen() > 0) {
                targetFiles.add(status.getPath());
            }
        }
        if (targetFiles.size() == 0) {
            LOG.error("no target file found.");
            System.exit(1);
        }

        FSDataOutputStream outputStream = fs.create(new Path(taskIn), true);
        for (Integer i = 0; i < targetFiles.size(); i++) {
            Path file = targetFiles.get(i);
            String targetName = file.getName().substring(0, file.getName().lastIndexOf('.'));
            String srcFilePath = targetFiles.get(i).toUri().getPath();
            String targetFilePath = outPath + "/" + targetName;
            outputStream.write(srcFilePath.getBytes());
            outputStream.write('\t');
            outputStream.write(targetFilePath.getBytes());
            outputStream.write('\n');
        }
        outputStream.close();

        //删除输出目录
        fs.delete(new Path(taskOut), true);

        Job job = Job.getInstance(conf);
        job.setJarByClass(MapReduceClient.class);

        //job执行作业时输入和输出文件的路径
        FileInputFormat.addInputPath(job, new Path(taskIn));
        FileOutputFormat.setOutputPath(job, new Path(taskOut));

        //每行一个map任务
        job.setInputFormatClass(NLineInputFormat.class);
        job.getConfiguration().set(NLineInputFormat.LINES_PER_MAP, "1");
        job.setNumReduceTasks(0);

        //指定自定义的Mapper和Reducer作为两个阶段的任务处理类
        job.setMapperClass(Mapper.class);

        //设置最后输出结果的Key和Value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        //执行job，直到完成
        job.waitForCompletion(true);

        //合并map输出目录
        outputStream = fs.append(new Path(taskIn));
        statuses = fs.globStatus(new Path(taskOut+"/*"));
        outputStream.write("----result-------\n".getBytes());
        for (Integer i = 0; i < statuses.length; i++) {
            FileStatus status = statuses[i];
            if (status.isFile() && status.getLen() > 0) {
                FSDataInputStream inputStream= fs.open(status.getPath());
                IOUtils.copyBytes(inputStream, outputStream, conf.getInt("io.file.buffer.size", 4096), false);
                IOUtils.closeStream(inputStream);
            }
        }
        IOUtils.closeStream(outputStream);

        fs.deleteOnExit(new Path(taskOut));
        System.out.println("Finished");

    }

}