import java.io.*;
import java.text.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Problem2_WC {
	public static class MapperClass extends Mapper<LongWritable,Text,Text,IntWritable> {
		public void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException {
			String lines = value.toString();
			String[] parts = lines.split("\n");
			for(String val:parts){
				Text outputkey = new Text(val.toLowerCase().trim());
				IntWritable outputvalue = new IntWritable(1);
				con.write(outputkey, outputvalue);
			}
		}
	}
	
	public static class ReducerClass extends Reducer<Text,IntWritable,Text,IntWritable> {
		public void reduce(Text key,Iterable<IntWritable> values,Context con) throws IOException, InterruptedException {
			int sum=0;
			for(IntWritable val:values){
				sum+=val.get();
			}
			con.write(key, new IntWritable(sum));
		}
	}
	
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] files = new GenericOptionsParser(conf,args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		Job j = Job.getInstance(conf, "Problem2_WC");
		j.setJarByClass(Problem2_WC.class);
		j.setMapperClass(MapperClass.class);
		j.setReducerClass(ReducerClass.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true)?0:1);
	}
}
