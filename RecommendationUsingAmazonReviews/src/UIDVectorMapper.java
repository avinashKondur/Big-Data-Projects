package AmazonReviews;

import java.io.IOException;



import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UIDVectorMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		
		String tokens[] = value.toString().split("\t");
		String dummy = tokens[1]+"\t"+tokens[2];
		context.write(new Text(tokens[0]), new Text(dummy));	
	}	
}
