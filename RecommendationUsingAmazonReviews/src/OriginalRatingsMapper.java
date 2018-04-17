package AmazonReviews;

import java.io.IOException;



import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OriginalRatingsMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		
		String tokens[] = value.toString().split("\t");
		String uid = tokens[0];
		
		String dummy = tokens[1]+"\t"+tokens[2]+"\t"+tokens[3];
		
		context.write(new Text(uid), new Text(dummy));	
	}	
}
