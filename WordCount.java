package org.apache.hadoop.examples;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {
    public static class MyMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, LongWritable>{
        @Override
        protected void map(LongWritable key, Text value,
        				   Mapper<LongWritable, Text, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split("\\s|\"|'|\\,|\\(|\\)");
            for (String word : words) {
                context.write(new Text(word), new LongWritable(1));
            }
        }
    }

    public static class MyReducer extends org.apache.hadoop.mapreduce.Reducer<Text, LongWritable, Text, LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values,
                			  Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            long count = 0L;
            for (LongWritable value : values) {
                count += value.get();
            }
            LongWritable out_value = new LongWritable(count);
            context.write(key, out_value);
        }
    }

    public static class InverseMapper 
    extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, LongWritable, Text>
    {
        @Override
        protected void map(LongWritable key, Text value,
        				   Mapper<LongWritable, Text, LongWritable, Text>.Context context)
                throws IOException, InterruptedException {
        	String line = value.toString();
        	String[] words = line.split("\t");
        	String word = words[0];
        	int count = Integer.parseInt(words[1]);
        	context.write(new LongWritable(count), new Text(word));
        }
    }
    
    private static class LongWritableCompare extends LongWritable.Comparator {
        @Override  
        @SuppressWarnings("all")
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
         }
   }
    
	//public static final String INPUT_PATH = "hdfs://localhost:9000/user/ifan/test/hello.txt";
	//public static final String OUTPUT_PATH = "hdfs://localhost:9000/user/ifan/test/output";

    //在命令行输入两个参数input_path, output_path
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        try {
            int res = ToolRunner.run(conf, new WordCount(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

	@Override
	public int run(String[] arg0) throws Exception {
        //Configuration conf = new Configuration();
		Configuration conf = getConf();
		final String input_path = arg0[0];
        Path outDir = new Path(arg0[1]);
        Path tempDir = new Path("wordcount-temp-output");
        
        FileSystem fileSystem = FileSystem.get(new URI(input_path), conf);
        if (fileSystem.exists(outDir)) {
        	fileSystem.delete(outDir, true);
        }

        Job job = Job.getInstance(conf, WordCount.class.getSimpleName());
        job.setJarByClass(WordCount.class);
        
        //输入
        FileInputFormat.setInputPaths(job, input_path);

        //mapper及参数
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        
        //reducer及参数
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileOutputFormat.setOutputPath(job, tempDir);
        
        //交给yarn去执行，直到执行结束才退出本程序
        job.waitForCompletion(true);
        
        Job sortjob = Job.getInstance(conf, "sort");
        FileInputFormat.setInputPaths(sortjob, tempDir);
        sortjob.setInputFormatClass(TextInputFormat.class);
        sortjob.setMapperClass(InverseMapper.class);
        sortjob.setNumReduceTasks(1);

        FileOutputFormat.setOutputPath(sortjob, outDir);
        sortjob.setOutputKeyClass(LongWritable.class);
        sortjob.setOutputValueClass(Text.class);
        sortjob.setSortComparatorClass(LongWritableCompare.class);
        sortjob.waitForCompletion(true);
        FileSystem.get(conf).delete(tempDir, true);
        System.exit(0);
        return 0;
	}
}
