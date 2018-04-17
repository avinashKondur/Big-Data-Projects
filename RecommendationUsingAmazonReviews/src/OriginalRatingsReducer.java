package AmazonReviews;


import java.io.IOException;
import java.util.HashMap;

//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OriginalRatingsReducer extends Reducer<Text,Text,Text,Text>{
	
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		/*Vector<String> uidVector = new Vector<String>();
		HashMap<String,String> localPid=new HashMap<String,String>();
		double avgRating=0.0;
		long prodCount = 0;
		for(Text value : values){
			String[] args = value.toString().split("\t");
			String pid = args[0];
			double rating = Double.parseDouble(args[1]);
			prodCount++;
			avgRating = avgRating + rating;
			String dummy = args[1]+"\t"+args[2];
			if(!localPid.containsKey(pid))
				localPid.put(pid, dummy);
        }
		avgRating = avgRating/prodCount;
		for(String s: MainClass.pids.keySet()){
			if(localPid.containsKey(s))
			{
				String[] vals = localPid.get(s).split("\t");
				Double rating = (Double.parseDouble(vals[0]))-avgRating;
				String sentiment = vals[1];
				if(rating > 0 && sentiment.equalsIgnoreCase("P")){
					uidVector.add(rating.toString());
				}
				else if(rating < 0 && sentiment.equalsIgnoreCase("N")){
					uidVector.add(rating.toString());
				}
				else if(rating == 0 && sentiment.equalsIgnoreCase("X")){
					uidVector.add(rating.toString());
				}	
				else{ 
					uidVector.add("N/A");
				}	
			}
			else
			{
				uidVector.add("-");
			}
		}    */
		HashMap<String,String> localPid=new HashMap<String,String>();
		double avgRating=0.0;
		long prodCount = 0;
		for(Text value : values){
			String[] args = value.toString().split("\t");
			double rating = Double.parseDouble(args[1]);
			String pid = args[0];
			prodCount++;
			avgRating = avgRating + rating;
			if(!localPid.containsKey(pid)){
				localPid.put(pid,args[1]+ "\t"+args[2]);
			}
        }
		avgRating = avgRating/prodCount;
		
		for(String s: localPid.keySet()){
			
			String[] vals = localPid.get(s).split("\t");
			double rating = Double.parseDouble(vals[0])-avgRating;
			String sentiment = vals[1];
			if((rating>0 && sentiment.equalsIgnoreCase("positive")) 
					|| (rating<0 && sentiment.equalsIgnoreCase("negative")) 
					|| (rating==0 && sentiment.equalsIgnoreCase("neutral"))
					|| (rating>0 && rating <1 && sentiment.equalsIgnoreCase("neutral")) 
					|| sentiment.equalsIgnoreCase("#####")){
				
				 context.write(key, new Text(s+"\t"+Double.toString(rating)));
				
			}
			else
				context.write(key, new Text(s+"\t"+"N/A"));
				
		}
       
       
     
	}    	
		
}