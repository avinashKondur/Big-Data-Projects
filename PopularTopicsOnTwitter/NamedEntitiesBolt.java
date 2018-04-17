import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import twitter4j.Status;

@SuppressWarnings("serial")
public class NamedEntitiesBolt extends BaseRichBolt {

	private OutputCollector _collector;

	
	private FileWriter _fileWriter ;
	private String _fileName;
	private BufferedWriter _bw;
	
	private Boolean _generateFile;
	
	/*
	 * Identifies the Named entities in a given text and returs the list of words
	 */
	private List<String> _getNamedEntities(String text) throws Exception {
		
		List<String> entities  = new ArrayList<String>();
		
		try
		{
			//.. load the nlp classifier for named entities identification
			String serializedClassifier = "edu/stanford/nlp/models/ner/english.muc.7class.distsim.crf.ser.gz"; 
			 AbstractSequenceClassifier _classifier = CRFClassifier.getClassifierNoExceptions(serializedClassifier);
			
			//classify the text
			@SuppressWarnings("unchecked")
			List<List<CoreLabel>> classifiedEntities = _classifier.classify(text);
	
						
			//iterate the result and print it.
			for (List<CoreLabel> sentence : classifiedEntities) {
			    for (CoreLabel word : sentence) {
			    	
			    	//.. we will ignore the ones that does not contain any classes
			    	 if(word.getString(CoreAnnotations.AnswerAnnotation.class).equals("O"))
				            continue;
			        
			    	 entities.add(word.word().toLowerCase());
			    }
	
			}
		}
		catch(Exception e) {
			
			e.printStackTrace();
			
		}
		
		return entities;
	}
	
	@Override
	public void execute(Tuple tuple) {
		
		Status tweet = (Status) tuple.getValueByField(EntityConstants.TWEET);
		long score = tuple.getLongByField(EntityConstants.SENTI_SCORE);
		
		List<String> entities;
		try {
			
			entities = _getNamedEntities(tweet.getText());
			
			for(String entity : entities) {
				
				this._collector.emit(new Values(entity,score));
				
				System.out.print("emitted the entity  = " + entity);
				
				try {
					_fileWriter 	= new FileWriter(_fileName,true);
					BufferedWriter _bw 		= new BufferedWriter(_fileWriter);			
										
					_bw.write(entity + ":" + score);
					_bw.write("\n");
					
					_bw.flush();
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				
			}
			
		} catch (Exception e) {
			
			System.out.println("There is an exception while identifying Named Entities --> " + e.getMessage());
			
			e.printStackTrace();
		}
					
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		
		this._collector = collector;
		
		try {
			_fileName 		= "/s/chopin/l/grad/avik/workspace/CS535/Assignment2/namedEnitity.txt";
			_fileWriter 	= new FileWriter(_fileName,true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		declarer.declare(new Fields(EntityConstants.ENTITY,EntityConstants.SENTI_SCORE));
		
	}

}
