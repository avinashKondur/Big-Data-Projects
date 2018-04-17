import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ConsineSimilarityReducer extends Reducer<Text,Text,Text,Text>{

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException { 

		TreeMap<Double, String> tm = new TreeMap<Double, String>();


		for (Text value : values){

			String[] cos = value.toString().split("\t");

			tm.put(Double.parseDouble(cos[1]), cos[0] );

		}

		if(tm.size() >= 10){
			int i = 0;
			for(Double cos : tm.descendingKeySet()){

				if(i <10){
					context.write(new Text(Double.toString(cos)), new Text(tm.get(cos)));
					i++;
				}
				else
					break;
			}

		}
		else{

			for(Double cos : tm.descendingKeySet()){


				context.write(new Text(Double.toString(cos)), new Text(tm.get(cos)));

			}

		}

	}
}