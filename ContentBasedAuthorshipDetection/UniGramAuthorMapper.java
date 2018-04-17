
import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class UniGramAuthorMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException { 

		NGram gram = new NGram(values.toString());
	
		for (String g : gram.GetUniGrams()) {

			context.write(new Text(g),new Text(gram.GetAuthorLastName()));

		}

		
	}
	
}
