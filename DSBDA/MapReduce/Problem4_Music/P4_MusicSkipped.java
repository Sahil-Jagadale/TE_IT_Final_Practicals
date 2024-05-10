import java.io.*;

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


public class P4_MusicSkipped {
	public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			if(value.toString().startsWith("UserId")){
				return;
			}
			String list = value.toString();
			String[] parts = list.split("\n");
			for(String word:parts){
				String[] val = word.split(",");
				Text outputkey = new Text(val[1]);
				IntWritable outputvalue = new IntWritable(Integer.parseInt(val[4]));
				con.write(outputkey, outputvalue);
			}
		}
	}
	
	public static class ReducerClass extends Reducer<Text,IntWritable,Text,IntWritable> {
		protected void setup(Context con) throws IOException, InterruptedException {
			con.write(new Text("The Number of Times Track is skipped"), null);
	    }
		int cnt=0;
		public void reduce(Text key,Iterable<IntWritable> values,Context con) throws IOException, InterruptedException {
			int sum=0;
			for(IntWritable val:values){
				sum+=val.get();
			}
			con.write(key, new IntWritable(sum));
			cnt++;
		}
		
		protected void cleanup(Context con) throws IOException, InterruptedException {
			con.write(new Text("The no of unique tracks are : "),new IntWritable(cnt));
		}
	}
	
	
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] files = new GenericOptionsParser(conf,args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		Job j = Job.getInstance(conf, "P4_MusicSkipped");
		j.setJarByClass(P4_MusicSkipped.class);
		j.setMapperClass(MapperClass.class);
		j.setReducerClass(ReducerClass.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true)?0:1);
	}
}

