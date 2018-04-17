import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class UKnownMainClass {

	public static void main(String[] args)throws IOException, ClassNotFoundException,InterruptedException {

		if (args.length != 4) {
			System.out.println("Usage: <jar file> <IDF Dir> <AAV DIR> <input dir> <output Dir> \n");
			System.exit(-1);
		}



		// Calculating the Term frequency for the words
		Configuration conf = new Configuration();

		Job job1=Job.getInstance(conf);
		job1.setJarByClass(UKnownMainClass.class);
		job1.setMapperClass(TFMapper.class);
		job1.setReducerClass(TFReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job1, new Path(args[2]));
		FileOutputFormat.setOutputPath(job1, new Path(args[3]+"/TF"));
		job1.waitForCompletion(true);

		
		String count = null;		

		Path pt=new Path(args[0]+"/AuthorCount/part-r-00000");

		FileSystem fs = FileSystem.get(conf);
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		String line;
		line=br.readLine();
		if(line != null){
			String[] arg = line.split("\t");
			count = arg[0].trim();
		}
		fs.close();

		//.. finding TF IDF 
		conf = new Configuration();
		
		conf.set("AuthorCount", count);

		Job job2=Job.getInstance(conf);

		job2.setJarByClass(UKnownMainClass.class);
		job2.setMapperClass(TFIDFMapper.class);
		
		job2.setReducerClass(TFIDFReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job2, new Path(args[0]+"/IDF"));
		FileInputFormat.setInputPaths(job2, new Path(args[3]+"/TF"));		
		FileOutputFormat.setOutputPath(job2, new Path(args[3]+"/TFIDF"));
		job2.waitForCompletion(true);


		//.. finding AAV
		conf = new Configuration();
		conf.set("idfPath", args[0]+"/IDF/part-r-00000");

		Job job3=Job.getInstance(conf);

		job3.setJarByClass(UKnownMainClass.class);
		job3.setMapperClass(AAVMapper.class);
		job3.setReducerClass(AAVReducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job3, new Path(args[3]+"/TFIDF"));		
		FileOutputFormat.setOutputPath(job3, new Path(args[3]+"/AAV"));
		job3.waitForCompletion(true);
		
		conf = new Configuration();
		conf.set("unknownAAVPath", args[3]+"/AAV/part-r-00000");
		
		Job job4=Job.getInstance(conf);
		
		job4.setJarByClass(UKnownMainClass.class);
		job4.setMapperClass(CosineSimilarityMapper.class);
		job4.setReducerClass(ConsineSimilarityReducer.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		job4.setInputFormatClass(TextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job4, new Path(args[1]));		
		FileOutputFormat.setOutputPath(job4, new Path(args[3]+"/CosSimilarity"));
		System.exit(job4.waitForCompletion(true)? 0: 1);


	}

}
