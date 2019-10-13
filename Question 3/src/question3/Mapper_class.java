package question3;
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
	
	
	
	public static class MaxTemperatureMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		public void map(LongWritable arg0, Text Value, Context context)
				throws IOException, InterruptedException {
 
			
			String line = Value.toString();
			
		
			
			if (!(line.length() == 0)) {
			
				
				String date = line.substring(6, 14);
 				
				float temp_Maximum = Float
						.parseFloat(line.substring(39, 45).trim());
				
			
				
				float temp_Minimun = Float
						.parseFloat(line.substring(47, 53).trim());

				
				if (temp_Maximum > 35.0) {
					
					context.write(new Text("Hot Day " + date),
							new Text(String.valueOf(temp_Maximum)));
				}

				
				if (temp_Minimun < 10) {
					context.write(new Text("Cold Day " + date),
							new Text(String.valueOf(temp_Minimun)));
				}
			}
		}
 
	}
}