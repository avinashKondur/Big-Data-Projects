import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AAVReducer extends Reducer<Text,Text,Text,Text>{
	
	HashMap<String,String> wordIdf = new HashMap<String,String>();
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException{		
		
		
		Configuration conf = context.getConfiguration();
		
		System.out.println("=========================idf path========================="+conf.get("idfPath"));
		
		Path idfPath = new Path(conf.get("idfPath"));

		FileSystem fs = FileSystem.get(conf);

		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(idfPath)));

		//String line;
		String line=br.readLine();

		while(line != null){
			String[] data = line.split("\t");
			wordIdf.put(data[0].trim(),data[1].trim());
			line=br.readLine();
		}
		//fs.close();
		//br.close();
		
		
	}
		
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException { 

		
		Vector<String> aav = new Vector<String>();
		
		HashMap<String, String> map = new HashMap<String, String>();
		
		for(Text value : values){
			
			String[] data = value.toString().split("\t");
			String w = data[0];				
			String tfidf = data[1];
			if(!map.containsKey(w))
				map.put(w, tfidf);	
		}
		
		for(String k :wordIdf.keySet()){
			
			if(map.containsKey(k)){
				
				aav.add(map.get(k));
			}
			else
			{
				double idf = Double.parseDouble(wordIdf.get(k));
				idf = 0.5*idf;
				aav.add(Double.toString(idf));
			}
		}
				
	
		String AAV = aav.toString();
		//.. Author and his/her AAV
		context.write(key,new Text(AAV));
		
	}
}