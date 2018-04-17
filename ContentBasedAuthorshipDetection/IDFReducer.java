
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IDFReducer extends Reducer<Text, Text, Text, Text> {

	public String AuthorCount ="" ;
	@Override
	protected void setup(  Context context)throws IOException,InterruptedException
	{
		Configuration conf = context.getConfiguration();

		AuthorCount = conf.get("AuthorCount");

	}

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		//String[] counts = values.toString();

		for(Text line : values){
			System.out.println("============================ Author count ======================= " + AuthorCount);
			double ni = Double.parseDouble( line.toString());
			if(ni == 0){
				throw new IOException("No values");
			}
			double N =  Double.parseDouble(AuthorCount);

			double frac = N/ni;
			double idf = Math.log10(frac);

			System.out.println("============================ idf value  ======================= " + idf);

			context.write(key, new Text(Double.toString(idf)));
		}

	}

}
