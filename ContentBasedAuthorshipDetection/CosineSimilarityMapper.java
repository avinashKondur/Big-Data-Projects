import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CosineSimilarityMapper extends Mapper<LongWritable, Text, Text, Text>{

	String xaav = "";
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException{		
		
		
		Configuration conf = context.getConfiguration();
		
		
		//String str = "hdfs://annapolis:30241/outputData/unigramProfile/part-r-00000";
		
		System.out.println("=========================idf path========================="+conf.get("idfPath"));
		
		Path unknownAAVPath = new Path(conf.get("unknownAAVPath"));
	    
		FileSystem fs = FileSystem.get(conf);
	    
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(unknownAAVPath)));
		
	    String line;
	    line=br.readLine();
	    
	    while(line != null){
	            String[] data = line.split("\t");
	            xaav =data[1].trim();
	            line=br.readLine();
	    }
		//fs.close();
		//br.close();
		
	}
	public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException { 
		
		String[] value = values.toString().split("\t");
		
		String author = value[0];
		String[] aav = value[1].replaceAll("\\[", "").replaceAll("\\]","").split(",");
		
		//.. Calculating Cosine similarity
		String[] uAAV = xaav.replaceAll("\\[", "").replaceAll("\\]","").split(",");
		
		Double AdotB = 0.0;
		Double ASquare = 0.0;
		Double BSquare = 0.0;
		
		for (int i = 0; i < uAAV.length; i++)
		{
			AdotB += Double.parseDouble(aav[i]) * Double.parseDouble(uAAV[i]);
			ASquare += Math.pow(Double.parseDouble(aav[i]),2);
			BSquare += Math.pow(Double.parseDouble(uAAV[i]),2);
		}
		
		ASquare = Math.sqrt(ASquare);
		BSquare = Math.sqrt(BSquare);
		
		Double cosSimiliarity = (AdotB)/(ASquare * BSquare);
	
		context.write(new Text("##########"), new Text( author + "\t" +Double.toString(cosSimiliarity)));
		
	}
}
