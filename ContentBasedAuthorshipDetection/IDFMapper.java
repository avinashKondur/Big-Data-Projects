
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class IDFMapper extends Mapper<LongWritable, Text, Text, Text>{


	public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException { 
		
		String[] wordAuthorCount = values.toString().split("\t");
		
		Text counts = new Text(wordAuthorCount[1]);
		
		String word = wordAuthorCount[0];
		
		context.write(new Text(word), counts);
		
	}
}
