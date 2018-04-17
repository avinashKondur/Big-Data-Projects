package AmazonReviews;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SentimentReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		for (Text value : values) {

			HashMap<String, Integer> hm = new HashMap<String,Integer>();

			String[] data = value.toString().split("\t");

			String[] sentiments = data[2].split(",");

			for(String sent : sentiments){

				if(hm.containsKey(sent)){
					int count = hm.get(sent);
					count = count+1;

					hm.replace(sent, count);

				}
				else {
					hm.put(sent,1);
				}

			}

			if(hm.containsKey("#####")){

				StringBuilder str = new StringBuilder();

				str.append(data[0]).append("\t").append(data[1]).append("\t").append(data[2]);

				context.write(key,new Text(str.toString()));
			}
			else{
				
				int PosCount;
				int NegCount;
				int NeutralCount;
				
				if(!hm.containsKey("Positive") && !hm.containsKey("Very positive"))
					PosCount = 0;
				else if(!hm.containsKey("Positive"))
					PosCount =  hm.get("Very positive");
				else if(!hm.containsKey("Very positive"))
					PosCount = hm.get("Positive");
				else	
					PosCount = hm.get("Positive") + hm.get("Very positive");
				
				if(!hm.containsKey("Negative") && !hm.containsKey("Very negative"))
					NegCount = 0;
				else if(!hm.containsKey("Negative"))
					NegCount =  hm.get("Very negative");
				else if(!hm.containsKey("Very negative"))
					NegCount = hm.get("Negative");
				else	
					NegCount = hm.get("Negative") + hm.get("Very negative");
				
				if(!hm.containsKey("Neutral"))
					NeutralCount = 0;
				else
					NeutralCount = hm.get("Neutral");
				
				
				String finalSent = "";
				if(NeutralCount > PosCount && NeutralCount > NegCount)
					finalSent = "Neutral";
				else if(NeutralCount+PosCount > NeutralCount+NegCount){

					finalSent = "Positive";
				}
				else if (NeutralCount+PosCount < NeutralCount+NegCount){
					finalSent = "Negative";
				}
				else{
					finalSent = "Neutral";
				}

				StringBuilder str = new StringBuilder();

				str.append(data[0]).append("\t").append(data[1]).append("\t").append(finalSent);

				context.write(key,new Text(str.toString()));
			}

		}
	}
}
