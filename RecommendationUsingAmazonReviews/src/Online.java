package AmazonReviews;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Online {
	public static HashMap<String,String> pids=new HashMap<String,String>();
	public static void main(String[] args) throws IOException, ClassNotFoundException,InterruptedException {



		Configuration conf =new Configuration();
		conf.set("mapreduce.job.maps", "100");
		conf.set("mapreduce.job.reduces", "100");
		conf.set("mapreduce.map.output.compress", "true");
		conf.set("mapreduce.map.java.opts", "-Xmx1024M");
		Job job=Job.getInstance(conf);
		job.setJarByClass(Offline.class);
		job.setMapperClass(SentimentMapper.class);
		job.setReducerClass(SentimentReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"/FinalSentimentsOnline"));
		job.waitForCompletion(true);


		
		Job job3=Job.getInstance(conf);
		job3.setJarByClass(Offline.class);
		job3.setMapperClass(OriginalRatingsMapper.class);
		job3.setReducerClass(OriginalRatingsReducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job3, new Path(args[1]+"/FinalSentimentsOnline"));
		FileOutputFormat.setOutputPath(job3, new Path(args[1]+"/originalRatingsOnline"));
		job3.waitForCompletion(true);

		conf = new Configuration();
		Job job1=Job.getInstance(conf);
		job1.setJarByClass(Offline.class);
		job1.setMapperClass(UIDVectorMapper.class);
		job1.setReducerClass(UIDVectorReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job1, new Path(args[1]+"/originalRatingsOnline"));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]+"/UIDVectorOnline"));
		job1.waitForCompletion(true);


		Job job2=Job.getInstance(conf);
		job2.setJarByClass(Online.class);
		job2.setMapperClass(RecommendationMapper.class);
		job2.setReducerClass(RecommendationReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.addCacheFile(new Path(args[1]+"/UIDVectorOnline/part-r-00000").toUri());
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job2, new Path(args[1]+"/TPPIDSim"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/RecommendedProducts"));

		System.exit(job2.waitForCompletion(true) ? 0 : 1);



	}

}