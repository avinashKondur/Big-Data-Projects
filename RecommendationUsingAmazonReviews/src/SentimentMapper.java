package AmazonReviews;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class SentimentMapper extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


		String[] data = value.toString().split("\t");
		
		String str = data[1]+"\t"+data[2]+"\t"+data[3];
		
		context.write(new Text(data[0]), new Text(str.toString()) );
		
		
		
	}

}
