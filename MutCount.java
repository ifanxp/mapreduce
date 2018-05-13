package org.apache.hadoop.examples;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * 参考资料：http://www.cnblogs.com/edisonchou/p/4288737.html
 * 从hdfs中读取csv格式的文件，统计并输出
 * csv文件格式：手机型号,os,sdk,uv,人均时长
 * 统计：根据手机型号分组，统计总uv和总人均时长
 */
public class MutCount extends Configured implements Tool {
    
    //自定义Writable
    public static class MutdataWritable implements Writable {
        Text model;
        Text os;
        Text sdk;
        LongWritable uv;
        FloatWritable duration;
        
        @Override
        public String toString(){
            return this.model + ", " + os + ", " + sdk + ", " + uv.toString() + ", " + duration.toString();
        }
        
        public MutdataWritable (){
        }
        
        public MutdataWritable(String _model, String _os, String _sdk, String _uv, String _duration) {
            try {
                this.model = new Text(_model.toLowerCase());
                this.os = new Text(_os);
                this.sdk = new Text(_sdk);
                this.uv = new LongWritable(Long.parseLong(_uv));
                this.duration = new FloatWritable(Float.parseFloat(_duration));
            }
            catch (NumberFormatException ex) {
                this.model = new Text("");
                this.os = new Text("");
                this.sdk = new Text("");
                this.uv = new LongWritable(0L);
                this.duration = new FloatWritable(0f);
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            try{
                this.model.readFields(in);
                this.os.readFields(in);
                this.sdk.readFields(in);
                this.uv.readFields(in);
                this.duration.readFields(in);
            }
            catch (NullPointerException ex) {
                this.model = new Text("");
                this.os = new Text("");
                this.sdk = new Text("");
                this.uv = new LongWritable(0L);
                this.duration = new FloatWritable(0f);
            }
        }

        @Override
        public void write(DataOutput out) throws IOException {
            this.model.write(out);
            this.os.write(out);
            this.sdk.write(out);
            this.uv.write(out);
            this.duration.write(out);
        }
    }
    
    public static class MyMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, MutdataWritable>{
        @Override
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, Text, MutdataWritable>.Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            MutdataWritable data = new MutdataWritable(fields[0], fields[1], fields[2], fields[3], fields[4]);
            System.out.println(data.toString());
            context.write(new Text(data.model), data);
        }
    }

    public static class MyReducer extends org.apache.hadoop.mapreduce.Reducer<Text, MutdataWritable, Text, MutdataWritable>{
        @Override
        protected void reduce(Text key, Iterable<MutdataWritable> values,
                              Reducer<Text, MutdataWritable, Text, MutdataWritable>.Context context) throws IOException, InterruptedException {
            long uv = 0L;
            float duration = 0f;
            for (MutdataWritable data : values) {
                System.out.println(data.toString());
                duration += data.uv.get() * data.duration.get(); 
                uv += data.uv.get();
            }
            String model = key.toString();
            String os = "";
            String sdk = "";
            float avgDuration = duration / uv;
            MutdataWritable result = new MutdataWritable(model, os, sdk, Long.toString(uv), Float.toString(avgDuration));
            context.write(key, result);
        }
    }

    public static final String INPUT_PATH = "hdfs://localhost:9000/user/ifan/test/oppo.CSV";
    public static final String OUTPUT_PATH = "hdfs://localhost:9000/user/ifan/test/output";

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();
        final String input_path = INPUT_PATH;
        Path outDir = new Path(OUTPUT_PATH);
        
        FileSystem fileSystem = FileSystem.get(new URI(input_path), conf);
        if (fileSystem.exists(outDir)) {
            fileSystem.delete(outDir, true);
        }

        Job job = Job.getInstance(conf, MutCount.class.getSimpleName());
        job.setJarByClass(MutCount.class);
        FileInputFormat.setInputPaths(job, input_path);

        //mapper及参数
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MutdataWritable.class);
        
        //reducer及参数
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MutdataWritable.class);
        FileOutputFormat.setOutputPath(job, outDir);
        
        //交给yarn去执行，直到执行结束才退出本程序
        job.waitForCompletion(true);
        System.exit(0);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        try {
            int res = ToolRunner.run(conf, new MutCount(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

