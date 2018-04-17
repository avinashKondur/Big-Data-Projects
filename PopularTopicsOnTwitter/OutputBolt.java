import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class OutputBolt extends BaseRichBolt {

	private OutputCollector _collector;
	private Map<String, EntityFrequency> _map ;	
	private String _logFileName;
	private Timestamp _startTime;
	
	private FileWriter _fileWriter ;
	private BufferedWriter _bw;
	
	
	public OutputBolt(String fileName) {
		
		this._logFileName = fileName;
		
	}
	
	@Override
	public void execute(Tuple tuple){
		
		EntityFrequency l = (EntityFrequency) tuple.getValueByField(EntityConstants.OUTPUT);// new EntityFrequency();

		/*String entity 	= tuple.getStringByField(EntityConstants.ENTITY);
		long freq 		= tuple.getLongByField(EntityConstants.LOSSY_COUNTS);
		long sentiScore = tuple.getLongByField(EntityConstants.SENTI_SCORE);*/
		
		Timestamp currentTime = new Timestamp(System.currentTimeMillis());
		
		long ms = currentTime.getTime()  - _startTime.getTime();
		
		int seconds  = (int)(ms/1000);
		
		if(seconds >= 10) {
			
			_writeDataToFile(currentTime);
			
			_deleteItems();
			
			_startTime = new Timestamp(System.currentTimeMillis());
			
			_map.put(l.Entity, l);	
		}
		else {
			_map.put(l.Entity, l);
		}
		
		
	}
	
	private  synchronized void _writeDataToFile(Timestamp currentTime) {
		
		Map<String, Long> counts = new HashMap();
		
		for(String key : _map.keySet()) {
			
			EntityFrequency l = _map.get(key);
			
			counts.put(key, l.Frequency);
		}
		
		System.out.println("counts.size() = " + counts.size());
		
		Map<String, Long> newCounts = counts.entrySet()
				        .stream()
				        .sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
				        .limit(100)
				        .collect(Collectors.toMap(
					          Map.Entry::getKey, 
					          Map.Entry::getValue, 
					          (e1, e2) -> e1, 
					          HashMap::new
				        ));
		
		System.out.println("Writing to the file " + _logFileName + ";  newCounts.size() = " + newCounts.size());
		
		if(newCounts.size() > 0) {
			StringBuilder builder = new StringBuilder();
			
			builder.append("<"+currentTime+">"+":");
			
			for ( String key : counts.keySet()) {
				
				EntityFrequency l = _map.get(key);
				//this._collector.emit(new Values(currentTime,key,l.SentiScore));
				
				builder.append("<"+key+":"+l.SentiScore + ">");
			}
			
			System.out.println("builder string to output - " + builder.toString());
			
			
			try {
				
				_fileWriter		= new FileWriter(_logFileName,true);
				BufferedWriter bw = new BufferedWriter(_fileWriter);

				bw.write(builder.toString());
				bw.write("\n");
				
				bw.flush();				
			} 
			catch (IOException e) {
				
				e.printStackTrace();
			}
		}
		
	}
	
	private void _deleteItems()
	{
		_map = new ConcurrentHashMap<String, EntityFrequency>();
		
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		
		this._collector = collector;
		_map 			= new ConcurrentHashMap<String, EntityFrequency>();
		_startTime		= new Timestamp(System.currentTimeMillis());
		try {
			_fileWriter		= new FileWriter(_logFileName,true);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		//declarer.declare(new Fields(EntityConstants.TIME_STAMP, EntityConstants.ENTITY,EntityConstants.SENTI_SCORE));
	}

	@Override
	public void cleanup() {


		try {
			
			_fileWriter.flush();
			_fileWriter.close();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
	}

}
