package AmazonReviews;


import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UIDReducer extends Reducer<Text,Text,Text,NullWritable>{
	
	
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		
		HashMap<String,String> localUid=new HashMap<String,String>();
		for(Text val : values){
			String s = val.toString();
			if(!localUid.containsKey(s)){
				localUid.put(s, "XXXXXXXXXXXXXX");
			}
		}
		for(String s: localUid.keySet()){
			context.write(new Text(s),NullWritable.get());
		}
						
			
       
       
     
	}    	
		
}