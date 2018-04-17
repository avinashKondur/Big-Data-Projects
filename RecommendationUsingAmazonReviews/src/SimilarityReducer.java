package AmazonReviews;


import java.io.IOException;

//import java.util.TreeMap;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SimilarityReducer extends Reducer<Text,Text,Text,Text>{



	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


		/*TreeMap<String, String> pidMap = 
				new TreeMap<String, String>();

		for(Text val : values){
			String[] s = val.toString().split("\t");
			s[1] = s[1].replace("}", "");
			s[1] = s[1].replace("{","").trim();
			if(!pidMap.containsKey(s[0])){
				pidMap.put(s[0],s[1]);
			}
		}
		for(String pid : pidMap.keySet()){
			String[] userRatingsMain = pidMap.get(pid).split(", ");
			TreeMap<String, String> pidComparisons = 
					new TreeMap<String, String>();
			for(String pidCompare : pidMap.keySet()){
				String[] urCompare = pidMap.get(pidCompare).split(", ");
				int i=0;
				int j=0;
				double similarity = 0.0;
				double nr = 0.0;
				double dr1 = 0.0;
				double dr2 = 0.0;
				int matches = 0;

				while(i<userRatingsMain.length && j<urCompare.length ){

					String[] x1 = userRatingsMain[i].split("=");
					String[] x2 = urCompare[j].split("=");
					if(x1.length>=2 && x2.length>=2){
						if(x1[0].trim().equalsIgnoreCase(x2[0].trim()) && !x1[1].equalsIgnoreCase("N/A")
								&& !x2[1].equalsIgnoreCase("N/A")){
							nr = nr + (Double.parseDouble(x1[1].trim())*Double.parseDouble(x2[1].trim()));
							dr1 = dr1 + (Double.parseDouble(x1[1].trim())*Double.parseDouble(x1[1].trim()));
							dr2 = dr2 + (Double.parseDouble(x2[1].trim())*Double.parseDouble(x2[1].trim()));
							i++;
							j++;
							matches++;
						}

						else if(x1[0].trim().compareToIgnoreCase(x2[0].trim()) < 0){
							i++;
						}

						else if(x1[0].trim().compareToIgnoreCase(x2[0].trim()) > 0){
							j++;
						}
					}

				}

				if(pid.equalsIgnoreCase(pidCompare)){
					similarity = 1.0;
				}				
				else if(nr == 0.0 && (dr1 != 0.0 || dr2 != 0.0)){
					similarity = 0.0;
				}
				else if(nr == 0.0 && dr1 == 0.0 && dr2 == 0.0){
					if(matches == 0){
						similarity = -1.0;
					}
					else 
						similarity = 1.0;
				}
				else if(dr1 !=0 && dr2!=0){
					dr1 = Math.sqrt(dr1);
					dr2 = Math.sqrt(dr2);
					similarity = nr / (dr1*dr2);
				}
				else{

				}
				if(!pidComparisons.containsKey(pidCompare)){
					pidComparisons.put(pidCompare, Double.toString(similarity));
				}
			}
			context.write(new Text(pid), new Text(pidComparisons.toString()));
		}    */
		for(Text value : values){
			String[] args = value.toString().split("\t");
			context.write(new Text(args[0]), new Text(args[1]));
		}	
	}    	

}