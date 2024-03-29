package question2;

import java.io.IOException;
import java.util.Iterator;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

public class Reducer_class {
/**
*MaxTemperatureReducer class is static and extends Reducer abstract class
having four hadoop generics type Text, Text, Text, Text.
*/

public static class MaxTemperatureReducer extends
		Reducer<Text, Text, Text, Text> {

	/**
	* @method reduce
	* This method takes the input as key and list of values pair from mapper, it does aggregation
	* based on keys and produces the final context.
	*/
	
	public void reduce(Text Key, Iterator<Text> Values, Context context)
			throws IOException, InterruptedException {

		
		//putting all the values in temperature variable of type String
		
		String temperature = Values.next().toString();
		context.write(Key, new Text(temperature));
	}

}


}
