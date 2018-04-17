package AmazonReviews;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;

public class SimilarityMapper extends Mapper<LongWritable, Text, Text, Text>{


	TreeMap<String,String> hm = new TreeMap<String,String>();
	@Override
	public void setup(Context context) throws IOException{
		URI[] uri=context.getCacheFiles();//Location of file in HDFS
		String path=uri[0].getPath();
		Path pt=new Path(path);
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		String line;
		line=br.readLine();
		while(line != null){
			String[] arg = line.split("\t");
			String uid = arg[0];
			String pidList = arg[1].replace("{", "");
			pidList = pidList.replace("}", "");
			if(!hm.containsKey(uid)){
				hm.put(uid, pidList);
			}
			line = br.readLine();
		}
	}


	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] args = value.toString().split("\t");
		String pid = args[0];
		args[1] = args[1].replace("{","");
		args[1] = args[1].replace("}", "");
		String[] pairs=args[1].split(", ");
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


		context.write(new Text("XXXXXXXXXXXXXXXX"),new Text(pid+"\t"+finalList.toString()));

	}


}
