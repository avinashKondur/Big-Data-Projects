package AmazonReviews;


import java.io.IOException;

import java.util.TreeMap;
import java.util.Vector;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RecommendationReducer extends Reducer<Text,Text,Text,Text>{
	
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		
	 // {"abc	1.0	{abc=1.0, def=1.0000000002, ghi=-1.0, jkl=-1.0}"};
		
		TreeMap<String,Double> recommendedItems=new TreeMap<String,Double>();
		ValueComparator bvc = new ValueComparator(recommendedItems);
	    TreeMap<String, Double> sortedMap = new TreeMap<String, Double>(bvc);
		Vector<String> existingItems = new Vector<String>();
		//int count = 0;
	
		for(Text val : values){
			
			String[] s = val.toString().split("\t");
			if(!existingItems.contains(s[0])){
				existingItems.add(s[0]);
			}
			if(s[1].equalsIgnoreCase("Not Applicable")){
				//count++;
			}
			else{
				double rating = Double.parseDouble(s[1]);
				s[2] = s[2].replace("{", "");
				s[2] = s[2].replace("}", "");
				String[] pairs = s[2].split(", ");
				for(int i=0;i<pairs.length;i++){
					String[] rate = pairs[i].split("=");
					if(Double.parseDouble(rate[1])*rating >= 0 ){
						if(!recommendedItems.containsKey(rate[0]))
							recommendedItems.put(rate[0], Double.parseDouble(rate[1])*rating);
						else{
							double prev = (recommendedItems.get(rate[0])+(Double.parseDouble(rate[1])*rating))/2;
							recommendedItems.replace(rate[0], prev);	
						}
					}	
				}
			}
			
		}
		
		sortedMap.putAll(recommendedItems);
		Vector<String> recommendations = new Vector<String>();
        int i=0;
        boolean flag=false;
		for(String s: sortedMap.keySet()){
			if(i==10 )
				break;
			else{
				if(!existingItems.contains(s)){
					
					recommendations.add(s);
					flag = true;
					i++;
				}
			}	
		}
		if(!flag || recommendations.size()==0){
			context.write(key,new Text("No products Recommended"));
		}
		else
			context.write(key,new Text(recommendations.toString()));

	}    	
		
}