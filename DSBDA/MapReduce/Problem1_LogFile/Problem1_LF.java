import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Problem1_LF {
	public static class LogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(",");
            
            if (parts.length >= 8) {
                String user = parts[1];
                String loginTimeStr = parts[5];
                String logoutTimeStr = parts[7];
                try {
                    Date loginTime = DATE_FORMAT.parse(loginTimeStr);
                    Date logoutTime = DATE_FORMAT.parse(logoutTimeStr);

                    long sessionDuration = logoutTime.getTime() - loginTime.getTime();
                    int sessionDurationMinutes = (int) (sessionDuration / (1000 * 60)); // Convert milliseconds to minutes

                    context.write(new Text(user), new IntWritable(sessionDurationMinutes));
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        }
    }
	
	public static class MaxDurationReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable maxDuration = new IntWritable();
        private Text user = new Text();
        int maxAmongAll = Integer.MIN_VALUE;

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int max = Integer.MIN_VALUE;
            for (IntWritable value : values) {
                int duration = value.get();
                if (duration > max) {
                    max = duration;
                }
            }
            maxDuration.set(max);
            context.write(key, maxDuration);
            if(max > maxAmongAll){
            	maxAmongAll=max;
            	user.set(key);
            }
        }
        
        protected void cleanup(Context context) throws IOException, InterruptedException {
        	context.write(new Text("User with maximum Logged Time is: "),null);
        	context.write(user,new IntWritable(maxAmongAll));
        }
    }

	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] files = new GenericOptionsParser(conf,args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		Job job = Job.getInstance(conf, "Problem1_LF");
		job.setJarByClass(Problem1_LF.class);
		job.setMapperClass(LogMapper.class);
		job.setReducerClass(MaxDurationReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}


