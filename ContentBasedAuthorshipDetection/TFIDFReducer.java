
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TFIDFReducer extends Reducer<Text, Text, Text, Text> {

	public String AuthorCount ="" ;
	@Override
	protected void setup(Context context)throws IOException,InterruptedException
	{
		Configuration conf = context.getConfiguration();

		AuthorCount = conf.get("AuthorCount");

	}
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		//String[] lines = values.toString().split("\n");

		//.. we should get only two lines from the mapper
		double idf=0 ;
		String author = "";
		double tf=0;
		for (Text line : values){

			String[] data = line.toString().split("\t");

			switch (data.length) {
			case 1:
				if(!data[0].trim().isEmpty())
					idf = Double.parseDouble( data[0].trim());	
				else
					idf = 0;
				break;

			case 2:
				tf = Double.parseDouble(data[1].trim());
				author = data[0].trim();
				break;
			default:
				break;
			}

		}

		if(idf == 0){
			idf = Math.log10(Double.parseDouble(AuthorCount));
		}

		double tfidf = tf*idf;

		context.write(key, new Text( author + "\t" + Double.toString(tfidf)));

	}

}
