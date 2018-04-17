package AmazonReviews;


import java.io.IOException;

import java.util.TreeMap;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PIDVectorReducer extends Reducer<Text,Text,Text,Text>{
	
	
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		
		TreeMap<String, String> pidMap = 
	             new TreeMap<String, String>();
		for(Text val : values){
			String[] s = val.toString().split("\t");
			if(!pidMap.containsKey(s[0])){
				pidMap.put(s[0],s[1]);
			}
		}
		context.write(key, new Text(pidMap.toString()));
			
       
       
     
	}    	
		
}