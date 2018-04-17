

import java.io.BufferedReader;

//import java.io.BufferedReader;
import java.io.IOException;
//import java.io.InputStreamReader;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MainClass {

	public static String Acount = "";
	public static HashMap<String,String> wordIdf = new HashMap<String,String>();
	public static void main(String[] args) throws IOException, ClassNotFoundException,InterruptedException {

		if (args.length != 2) {
			System.out.println("Usage: <jar file> <input dir> <output Dir> \n");
			System.exit(-1);
		}


		System.out.println("===============Starting TF calculation===========");
		// Calculating the Term frequency for the words
		Configuration conf = new Configuration();

		Job job1=Job.getInstance(conf);
		job1.setJarByClass(MainClass.class);
		job1.setMapperClass(TFMapper.class);
		job1.setReducerClass(TFReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]+"/TF"));
		job1.waitForCompletion(true);

		System.out.println("===============Starting Word Author count calculation===========");
		//.. calculating Word Author count
		conf = new Configuration();

		Job job2=Job.getInstance(conf);

		job2.setJarByClass(MainClass.class);
		job2.setMapperClass(WordAuthCountMapper.class);		
		job2.setReducerClass(WordAuthCountReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/WordAuthorCount"));
		job2.waitForCompletion(true);

		System.out.println("===============Starting author count calculation===========");
		// calculating author count
		conf = new Configuration();

		Job job3=Job.getInstance(conf);
		job3.setJarByClass(MainClass.class);
		job3.setMapperClass(AuthorCountMapper.class);
		job3.setReducerClass(AuthorCountReducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job3, new Path(args[0]));
		FileOutputFormat.setOutputPath(job3, new Path(args[1]+"/AuthorCount"));
		job3.waitForCompletion(true);

		String count = null;		

		Path pt=new Path(args[1]+"/AuthorCount/part-r-00000");

		FileSystem fs = FileSystem.get(conf);
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		String line;
		line=br.readLine();
		if(line != null){
			String[] arg = line.split("\t");
			count = arg[0].trim();
		}
		fs.close();

		System.out.println("===============Starting IDF calculation==========="+count);
		conf = new Configuration();

		conf.set("AuthorCount", count);
		Acount = count;

		//.. Finding IDF
		Job job4=Job.getInstance(conf);

		job4.setJarByClass(MainClass.class);
		job4.setMapperClass(IDFMapper.class);
		job4.setReducerClass(IDFReducer.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		job4.setInputFormatClass(TextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job4, new Path(args[1]+"/WordAuthorCount"));
		FileOutputFormat.setOutputPath(job4, new Path(args[1]+"/IDF"));
		job4.waitForCompletion(true);

		System.out.println("===============Starting TFIDF calculation===========");
		//.. finding TF IDF 
		conf = new Configuration();

		conf.set("AuthorCount", count);
		Job job5=Job.getInstance(conf);

		job5.setJarByClass(MainClass.class);
		job5.setMapperClass(TFIDFMapper.class);
		job5.setReducerClass(TFIDFReducer.class);
		job5.setOutputKeyClass(Text.class);
		job5.setOutputValueClass(Text.class);
		job5.setInputFormatClass(TextInputFormat.class);
		job5.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job5, new Path(args[1]+"/IDF"));
		FileInputFormat.setInputPaths(job5, new Path(args[1]+"/TF"));		
		FileOutputFormat.setOutputPath(job5, new Path(args[1]+"/TFIDF"));
		job5.waitForCompletion(true);

		System.out.println("===============Starting AAV calculation===========");
		//.. finding AAV
		conf = new Configuration();
		//idfPath = new Path(args[1]+"/IDF/part-r-00000");
		conf.set("idfPath", args[1]+"/IDF/part-r-00000");
		
		Job job6=Job.getInstance(conf);

		job6.setJarByClass(MainClass.class);
		job6.setMapperClass(AAVMapper.class);
		job6.setReducerClass(AAVReducer.class);
		job6.setOutputKeyClass(Text.class);
		job6.setOutputValueClass(Text.class);
		job6.setInputFormatClass(TextInputFormat.class);
		job6.setOutputFormatClass(TextOutputFormat.class);		
		FileInputFormat.setInputPaths(job6, new Path(args[1]+"/TFIDF"));
		FileOutputFormat.setOutputPath(job6, new Path(args[1]+"/AAV"));		
		System.exit(job6.waitForCompletion(true) ? 0 : 1);
	}
}