package AmazonReviews;


import java.util.TreeMap;

public class SimilarityCalculator {

	public TreeMap<String,String> hm=new TreeMap<String,String>();

	public String PIDGenerator(String value){
		String[] args = value.split("\t");
		String pid = args[0];
		String[] pairs=args[1].replace("{","").replace("}", "").split(", ");
		TreeMap<String,String> finalList = new TreeMap<String,String>();
		for(String s: hm.keySet()){
			String[] comparePairs = hm.get(s).split(", ");
			int i=0,j=0,matches =0;
			double nr=0,dr1=0,dr2=0,similarity = 0;
			for(;i<pairs.length && j<comparePairs.length;){

				String[] x = pairs[i].split("=");
				String[] y = comparePairs[j].split("=");
				if(x.length!=0 && y.length !=0){
					if(x[0].equalsIgnoreCase(y[0]) &&!x[1].equalsIgnoreCase("N/A")
							&& !y[1].equalsIgnoreCase("N/A")){
						nr = nr + (Double.parseDouble(x[1].trim())*Double.parseDouble(y[1].trim()));
						dr1 = dr1 + (Double.parseDouble(x[1].trim())*Double.parseDouble(y[1].trim()));
						dr2 = dr2 + (Double.parseDouble(x[1].trim())*Double.parseDouble(y[1].trim()));
						i++;
						j++;
						matches++;
					}
					else if(x[0].trim().compareToIgnoreCase(y[0].trim()) < 0)
						i++;
					else if(x[0].trim().compareToIgnoreCase(y[0].trim()) > 0){
						j++;
					}
					else{
						if(x[1].equalsIgnoreCase("N/A"))
							i++;
						if(y[1].equalsIgnoreCase("N/A"))
							j++;
					}
				}
			}
			if(pid.equalsIgnoreCase(s)){
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
			if(!finalList.containsKey(s)){
				finalList.put(s, Double.toString(similarity));
			}

		}
		return pid+"\t"+finalList.toString();

	}

	public void UpdateSimilarity(String pid,String pidVector){

		if(!hm.containsKey(pid)){
			
			hm.put(pid, pidVector);
		}
	}

}
