package question1;

public class Reducer_class {
public static class TempReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {

int maxValue = 0;

//Looping and calculating Max for each year
for (IntWritable val : values) {
maxValue = Math.max(maxValue, val.get());
}

context.write(key, new IntWritable(maxValue));
}
}