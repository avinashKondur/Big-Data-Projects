

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class TFIDFMapper extends Mapper<LongWritable, Text, Text, Text>{

	public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException { 
		
		
		String[] f = values.toString().split("\t");
		
		// if length is two then the input is  word-IDF 
		if(f.length == 2)
		{
			//key = word value = idf
			context.write(new Text(f[0].trim()), new Text(f[1].trim()));
			
		}
		
		// if length is three then the input is author - word - tf
		if(f.length == 3)
		{
			//key = word  value = author tf 
			context.write(new Text(f[1].trim()), new Text(f[0].trim() + "\t" + f[2].trim()));
			
		}
		
	}
}
