package question4;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


//import question1.Driverclass.MaxTemperatureMapper;
//import question1.Driverclass.MaxTemperatureReducer;
public class Driver  {
	public static void main(String[] args) throws Exception {

		
		Configuration conf = new Configuration();
		
		//Initializing the job with the default configuration of the cluster		
		Job job = new Job(conf, "weather example");
		
		//Assigning the driver class name
		job.setJarByClass(Driverclass.class);

		//Key type coming out of mapper
		job.setMapOutputKeyClass(Text.class);
		
		//value type coming out of mapper
		job.setMapOutputValueClass(Text.class);

		//Defining the mapper class name
		job.setMapperClass(Mapperclass.class);
		
		//Defining the reducer class name
		job.setReducerClass(Reducerclass.class);

		//Defining input Format class which is responsible to parse the dataset into a key value pair
		job.setInputFormatClass(TextInputFormat.class);
		
		//Defining output Format class which is responsible to parse the dataset into a key value pair
		job.setOutputFormatClass(TextOutputFormat.class);

		//setting the second argument as a path in a path variable
		Path OutputPath = new Path(args[1]);

		//Configuring the input path from the filesystem into the job
		FileInputFormat.addInputPath(job, new Path(args[0]));

		//Configuring the output path from the filesystem into the job
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//deleting the context path automatically from hdfs so that we don't have delete it explicitly
		OutputPath.getFileSystem(conf).delete(OutputPath);

		//exiting the job only if the flag value becomes false
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}