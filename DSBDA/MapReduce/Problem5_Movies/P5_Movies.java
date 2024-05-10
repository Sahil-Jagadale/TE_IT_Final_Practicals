import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class P5_Movies {
	public static class MapperClass extends Mapper<LongWritable,Text,Text,FloatWritable> {
		public void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException {
			String list = value.toString();
			if(list.startsWith("userId")){
				return;
			}
			String parts[] = list.split("\n");
			for(String word:parts){
				String[] data = word.split(",");
				Text outputkey = new Text(data[1]);
				FloatWritable outputvalue = new FloatWritable(Float.parseFloat(data[2]));
				con.write(outputkey, outputvalue);
			}
		}
	}
	public static class ReducerClass extends Reducer<Text,FloatWritable,Text,FloatWritable> {
		Text maxrated = new Text();
		float maxRatting = Float.MIN_VALUE;
		TreeMap<Float,String> mp = new TreeMap<>();
		
		
		public void reduce(Text key,Iterable<FloatWritable> values,Context con) throws IOException, InterruptedException {
			float sum=0;
			float cnt=0;
			for(FloatWritable val:values){
				sum+=val.get();
				cnt++;
			}
			sum=(sum/cnt);
			
			mp.put(sum, key.toString());
			
			if(sum > maxRatting){
				maxRatting = sum;
				maxrated.set(key);
			}
			
		}
		
		protected void cleanup(Context con) throws IOException, InterruptedException {
			con.write(new Text("The movie with maximum rating is: "), null);
			con.write(maxrated, new FloatWritable(maxRatting));
			
			con.write(new Text("\n\nTop 10 movies according to their ratings are: "),null);
			int count=0;
			for(Map.Entry<Float, String> entry : mp.descendingMap().entrySet()){
				con.write(new Text(entry.getValue()),new FloatWritable(entry.getKey()));
				count++;
				if(count==10){
					break;
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] files = new GenericOptionsParser(conf,args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		Job j = Job.getInstance(conf, "P5_Movies");
		j.setJarByClass(P5_Movies.class);
		j.setMapperClass(MapperClass.class);
		j.setReducerClass(ReducerClass.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true)?0:1);
	}
}
