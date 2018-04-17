package AmazonReviews;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
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


public class Offline {


	public static void main(String[] args) throws IOException, ClassNotFoundException,InterruptedException {



		Configuration conf =new Configuration();
		//conf.set("mapreduce.job.maps", "100");
		//conf.set("mapreduce.job.reduces", "100");
		//conf.set("mapreduce.map.output.compress", "true");
		//conf.set("mapreduce.map.java.opts", "-Xmx1024M");
		Job job=Job.getInstance(conf);
		job.setJarByClass(Offline.class);
		job.setMapperClass(SentimentMapper.class);
		job.setReducerClass(SentimentReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"/FinalSentiments"));
		job.waitForCompletion(true);

		conf = new Configuration();




		//	conf = new Configuration();
		//conf.set("mapreduce.job.maps", "100");
		//conf.set("mapreduce.job.reduces", "100");
		//conf.set("mapreduce.map.output.compress", "true");
		//conf.set("mapreduce.map.java.opts", "-Xmx1024M");
		Job job2=Job.getInstance(conf);
		job2.setJarByClass(Offline.class);
		job2.setMapperClass(OriginalRatingsMapper.class);
		job2.setReducerClass(OriginalRatingsReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job2, new Path(args[1]+"/FinalSentiments"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/originalRatings"));
		job2.waitForCompletion(true);


		conf = new Configuration();
		//conf.set("mapreduce.job.maps", "100");
		//conf.set("mapreduce.job.reduces", "100");
		//conf.set("mapreduce.map.output.compress", "true");
		//conf.set("mapreduce.map.java.opts", "-Xmx1024M");
		Job job3=Job.getInstance(conf);
		job3.setJarByClass(Offline.class);
		job3.setMapperClass(PIDVectorMapper.class);
		job3.setReducerClass(PIDVectorReducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		//job3.setNumReduceTasks(2);
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job3, new Path(args[1]+"/originalRatings"));
		FileOutputFormat.setOutputPath(job3, new Path(args[1]+"/PIDVector"));
		job3.waitForCompletion(true);


		conf = new Configuration();
		conf.set("mapreduce.job.maps", "100");
		//conf.set("mapreduce.job.reduces", "100");
		conf.set("mapreduce.map.output.compress", "true");
		conf.set("mapreduce.map.java.opts", "-Xmx1536M");

		/*Job job4=Job.getInstance(conf);
		job4.setJarByClass(Offline.class);
		job4.setMapperClass(SimilarityMapper.class);
		job4.setReducerClass(SimilarityReducer.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		job4.addCacheFile(new Path(args[1]+"/PIDVector/part-r-00000").toUri());
		job4.setInputFormatClass(TextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job4, new Path(args[1]+"/PIDVector"));
		FileOutputFormat.setOutputPath(job4, new Path(args[1]+"/PIDSimilarities"));

		System.exit(job4.waitForCompletion(true) ? 0 : 1);*/

		SimilarityCalculator calc = new SimilarityCalculator();

		String str = "hdfs://annapolis:30241/"+args[1]+"/PIDVector/part-r-00000";
		//String str2 = "hdfs://annapolis:30241/TPOutput/PidSimilarities";
		Path pt=new Path(str);

		Path pt2=new Path("/TPOutput/");

		FileSystem fs = pt2.getFileSystem(conf);


		String outputFile = "/s/chopin/a/grad/cnreddy/Desktop/cs435/"+args[1]+"PIDSim";

		FileWriter fileWriter =
				new FileWriter(outputFile ,true);

		BufferedWriter bufferedWriter =
				new BufferedWriter(fileWriter);


		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));

		String line;
		System.out.println("Started Reading");
		//line=br.readLine();
		while((line = br.readLine())!= null){
			String[] arg = line.split("\t");
			String pid = arg[0];
			String pidVector = arg[1].replace("{","");
			pidVector = pidVector.replace("}", "");

			calc.UpdateSimilarity(pid,pidVector);			
		}

		System.out.println(" Done reading");
		br=new BufferedReader(new InputStreamReader(fs.open(pt)));

		//line=br.readLine();
		while((line = br.readLine()) != null){
			String writeLine = calc.PIDGenerator(line);
			bufferedWriter.write(writeLine);
			bufferedWriter.newLine();
		}
		System.out.println(" Done writing");
		bufferedWriter.close();

		Path srcPath = new Path(outputFile);
		Path destPath = new Path(args[1]+"/");
		System.out.println("Copying data");

		fs.copyFromLocalFile(srcPath, destPath);


	}



}