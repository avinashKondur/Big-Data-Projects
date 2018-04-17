import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;
import twitter4j.Status;

@SuppressWarnings("serial")
public class SentimentBolt extends BaseRichBolt  {

	private OutputCollector _collector;
	private StanfordCoreNLP _pipeline;
	private FileWriter _fileWriter;
	private String _fileName;
	
	
	private long GetSentimentScore(String text) throws Exception{
		 
		Properties properties 	= new Properties(); 		
		properties.setProperty("annotators", "tokenize, ssplit, pos, parse, sentiment"); 
		
		_pipeline 	= new StanfordCoreNLP(properties);
		
		Annotation document = new Annotation(text);
		_pipeline.annotate(document); 
		List<CoreMap> sentences = document.get(SentencesAnnotation.class);
		
		long score = 0;
		
		for (CoreMap sentence : sentences)
		{    
			String sentiment = sentence.get(SentimentCoreAnnotations.SentimentClass.class);  
			
			switch(sentiment.trim().toLowerCase()) {
			
				case "very negative":
					score += -2;
					break;
				case "negative":
					score += -1;
					break;
				case "neutral":
					break;
					
				case "positive":
					score += 1;
					break;
				case "very positive":
					score += 2;
					break;
				default:
					throw new IllegalArgumentException("Returned Sentiment =  {0}  is not in a valid list".format(sentiment.trim().toLowerCase()));  				
			} 
		}
		
		return score;
		
	}
	
	public void execute(Tuple tuple) {
		
		Status tweet = (Status)tuple.getValueByField(EntityConstants.TWEET);
		long sentiScore=0;
		
		try {
			
			sentiScore = GetSentimentScore(tweet.getText());
			
		} catch (Exception e) {
			
			System.out.println("Exception raised when identifying sentiment - "+e.getMessage());
			e.printStackTrace();			
		}
		
		this._collector.emit(new Values(tweet,sentiScore));
		
		
		try {
			_fileWriter 	= new FileWriter(_fileName,true);
			BufferedWriter _bw 		= new BufferedWriter(_fileWriter);			
								
			_bw.write(tweet.getText() + ":" + sentiScore);
			_bw.write("\n");
			
			_bw.flush();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {

		this._collector = collector;		
		try {
			_fileName 		= "/s/chopin/l/grad/avik/workspace/CS535/Assignment2/sentiment.txt";
			_fileWriter 	= new FileWriter(_fileName,true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		declarer.declare(new Fields(EntityConstants.TWEET,EntityConstants.SENTI_SCORE));
		
	}

	
	
}
