

import java.io.IOException;

import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TFReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		TreeMap<String, Integer> hm = new TreeMap<String, Integer>();

		int maxCount = 0;
		
		for (Text value : values) {	
			
			if (!hm.containsKey(value.toString())) {
				
				hm.put(value.toString(),1);
			}
			else{
				
				int count =  hm.get(value.toString());
				
				count = count+1;
				
				hm.put(value.toString(), count);
				
				if(count > maxCount)
					maxCount = count;
				
			}

		}
		
		//.. Calculating TF values
		for(String k: hm.keySet()){

			double tf =  0.5 + (0.5*(hm.get(k)/maxCount));
			
			context.write(new Text(key), new Text( k + "\t" + Double.toString(tf) ));
		}
		

	}

}
