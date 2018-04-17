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

public class RecommendationMapper extends Mapper<LongWritable, Text, Text, Text>{
	
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
	            String pidList = arg[1];
	            if(!hm.containsKey(uid)){
	            	hm.put(uid, pidList);
	            }
	            line = br.readLine();
	    }
	}
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String pidSim = value.toString().replace("{", "").replace("}", "");
		String[] pidSimilarities = pidSim.split("\t");
		for(String s: hm.keySet()){
			String temp = hm.get(s).replace("{", "");
			temp = temp.replace("}", "");
			String[] vals = temp.split(", ");
			TreeMap<String,String> hmLocal = new TreeMap<String,String>();
			for(int i=0;i<vals.length;i++){
				String[] args = vals[i].split("=");
				if(!hmLocal.containsKey(args[0]))
					hmLocal.put(args[0], args[1]);
			}
			if(hmLocal.containsKey(pidSimilarities[0])){
				if(hmLocal.get(pidSimilarities[0]).equalsIgnoreCase("n/a") || hmLocal.get(pidSimilarities[0]).equalsIgnoreCase("nan"))
					context.write(new Text(s), new Text(pidSimilarities[0]+"\tNot applicable"));
				else if(Double.parseDouble(hmLocal.get(pidSimilarities[0])) >= 0)
					context.write(new Text(s), new Text(pidSimilarities[0]+"\t"+
							Double.parseDouble(hmLocal.get(pidSimilarities[0])) + "\t" + pidSimilarities[1]));
			}
			if(hmLocal.isEmpty())
				context.write(new Text(s), new Text(pidSimilarities[0]+"\tNot applicable"));
		}
	}	
}
