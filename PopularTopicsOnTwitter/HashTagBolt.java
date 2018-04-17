import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import twitter4j.HashtagEntity;
import twitter4j.Status;

public class HashTagBolt extends BaseRichBolt {

	private OutputCollector _collector;
	
	private FileWriter _fileWriter ;
	private String _fileName;
	private BufferedWriter _bw;
	private Boolean _generateFile ;
	
	public HashTagBolt(Boolean generate) {
		
		_generateFile = generate;
	}
	
	@Override
	public void execute(Tuple tuple) {
		
		Status tweet = (Status) tuple.getValueByField(EntityConstants.TWEET);
		long sScore = tuple.getLongByField(EntityConstants.SENTI_SCORE);
		
		for(HashtagEntity hashTag : tweet.getHashtagEntities()) {
			
			String ht = "#"+hashTag.getText().toLowerCase();
			this._collector.emit(new Values(ht,sScore));
			
			if(_generateFile)
				try {
					_fileWriter 	= new FileWriter(_fileName,true);
					BufferedWriter _bw 		= new BufferedWriter(_fileWriter);			
										
					_bw.write(ht + ":" + sScore);
					_bw.write("\n");
					
					_bw.flush();
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
		}
		
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		
		this._collector = collector;
		
		if(_generateFile)
			try {
				_fileName 		= "/s/chopin/l/grad/avik/workspace/CS535/Assignment2/hashtag.txt";
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
