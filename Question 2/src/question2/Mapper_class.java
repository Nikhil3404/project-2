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
public class Mapper_class {
	//Mapper
	
		/**
		*MaxTemperatureMapper class is static and extends Mapper abstract class
		having four hadoop generics type LongWritable, Text, Text, Text.
		*/
		
		
		public static class MaxTemperatureMapper extends
				Mapper<LongWritable, Text, Text, Text> {
			
			/**
			* @method map
			* This method takes the input as text data type.
			* Now leaving the first five tokens,it takes 6th token is taken as temp_max and
			* 7th token is taken as temp_min. Now temp_max > 35 and temp_min < 10 are passed to the reducer.
			*/
	 
			@Override
			public void map(LongWritable arg0, Text Value, Context context)
					throws IOException, InterruptedException {
	 
			//Converting the record (single line) to String and storing it in a String variable line
				
				String line = Value.toString();
				
			//Checking if the line is not empty
				
				if (!(line.length() == 0)) {
					
					//date
					
					String date = line.substring(6, 14);
	 
					//maximum temperature
					
					float temp_Max = Float
							.parseFloat(line.substring(39, 45).trim());
					
					//minimum temperature
					
					float temp_Min = Float
							.parseFloat(line.substring(47, 53).trim());
	 
				//if maximum temperature is greater than 35 , its a hot day
					
					if (temp_Max > 35.0) {
						// Hot day
						context.write(new Text("Hot Day " + date),
								new Text(String.valueOf(temp_Max)));
					}
	 
					//if minimum temperature is less than 10 , its a cold day
					
					if (temp_Min < 10) {
						// Cold day
						context.write(new Text("Cold Day " + date),
								new Text(String.valueOf(temp_Min)));
					}
				}
			}
	 
		}
	 
	
}