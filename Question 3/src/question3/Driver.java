package question3;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
@SuppressWarnings("deprecation")
public static void main(String[] args) throws Exception {
	
 
                          
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "weather Data Question 3 ");
		
				job.setJarByClass(Driver.class);
 
				job.setMapOutputKeyClass(Text.class);
		
				job.setMapOutputValueClass(Text.class);
 
				job.setMapperClass(WeatherMapper.class);
		
				job.setReducerClass(WeatherReducer.class);
 
				job.setInputFormatClass(TextInputFormat.class);
		
				job.setOutputFormatClass(TextOutputFormat.class);
 
				Path OutputPath = new Path(args[1]);
 
				FileInputFormat.addInputPath(job, new Path(args[0]));
 
				FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
				OutputPath.getFileSystem(conf).delete(OutputPath);
 
				System.exit(job.waitForCompletion(true) ? 0 : 1);
 
	}
}