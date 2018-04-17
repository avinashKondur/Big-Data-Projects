import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AAVMapper extends Mapper<LongWritable, Text, Text, Text>{

	public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException { 

		String[] data = values.toString().split("\t");
		
		String word = data[0];
		String author = data[1];
		String tfidf = data[2];
		
		context.write(new Text(author), new Text(word +"\t"+tfidf));
			
	}
}

