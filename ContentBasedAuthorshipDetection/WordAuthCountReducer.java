
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;



public class WordAuthCountReducer extends Reducer<Text,Text,Text,Text>{
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException { 
		
		Set<String> set = new HashSet<String>();
		
		
		for (Text value : values){
			if(!set.contains(value.toString()))
				set.add(value.toString());
		}
		
		//.. word and its number of authors
		context.write(key, new Text(Integer.toString(set.size())));
		
	}
}