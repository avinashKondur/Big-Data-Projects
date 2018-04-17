package AmazonReviews;


import java.io.IOException;

import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UIDVectorReducer extends Reducer<Text,Text,Text,Text>{
	
	
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		
		TreeMap<String,String> uidVector=new TreeMap<String,String>();
		for(Text val : values){
			String[] s = val.toString().split("\t");
			if(!uidVector.containsKey(s[0])){
				uidVector.put(s[0], s[1]);
			}
		}
		
		context.write(key,new Text(uidVector.toString()));
		
						
			
       
       
     
	}    	
		
}