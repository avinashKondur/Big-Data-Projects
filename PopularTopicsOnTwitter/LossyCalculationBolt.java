import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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

public class LossyCalculationBolt extends BaseRichBolt{

	private OutputCollector _collector;
	
	private Map<String,EntityFrequency> _items;
	
	private double _epsilon ;	
	private long _bucketSize ;	
	private long _currentBucket ;
	private long _numItemsParsed;
	
	private FileWriter _fileWriter;
	private String _fileName;
	
	private double _threshold;
	
	//private Boolean _generateFile ;
	
	private Timestamp _startTime;
	
	public LossyCalculationBolt( double e, String fileName, double threshold) {
		
		//_generateFile 	= generate;
		_fileName		= fileName; 
		this._epsilon 			= e;
		this._threshold = threshold;
	}
	
	@Override
	public void execute(Tuple tuple) {
		
		String entity 		= tuple.getStringByField(EntityConstants.ENTITY);
		long score  		= tuple.getLongByField(EntityConstants.SENTI_SCORE);

		_updateLossyCount(entity, score);
		
		Timestamp currentTime = new Timestamp(System.currentTimeMillis());
		
		long ms = currentTime.getTime()  - _startTime.getTime();
		
		int seconds  = (int)(ms/1000);
		
		
		if(seconds >= 10) {
			
			System.out.println("Writng the data to file after 10 seconds");
			
			//_writeDataToFile(currentTime);
			
			_emitItems(_items);
			
			_startTime = new Timestamp(System.currentTimeMillis());
		}
		
	}
	
	private  synchronized void _writeDataToFile(Timestamp currentTime) {
		
		Map<String, Long> counts = new HashMap();
		
		for(String key : _items.keySet()) {
			
			EntityFrequency l = _items.get(key);
			
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
		
		System.out.println("Writing to the file " + _fileName + ";  newCounts.size() = " + newCounts.size());
		
		if(newCounts.size() > 0) {
			StringBuilder builder = new StringBuilder();
			
			builder.append("<"+currentTime+">"+":");
			
			for ( String key : counts.keySet()) {
				
				EntityFrequency l = _items.get(key);
				//this._collector.emit(new Values(currentTime,key,l.SentiScore));
				
				builder.append("<"+key+":"+l.SentiScore + ">");
			}
			
			System.out.println("builder string to output - " + builder.toString());
			
			
			try {
				
				_fileWriter		= new FileWriter(_fileName,true);
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


	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {		
		
		this._collector 	= collector;
		
		_items 				= new ConcurrentHashMap();
		
		//e 					= 0.002f;
		_bucketSize 		= (long) Math.ceil(1/_epsilon);
		_currentBucket  	= 1;
		_numItemsParsed		= 0 ;
		_startTime			= new Timestamp(System.currentTimeMillis());
		

			try {
				//_fileName 		= "/s/chopin/l/grad/avik/workspace/CS535/Assignment2/Lossy.txt";
				_fileWriter 	= new FileWriter(_fileName,true);
			} catch (IOException e) {
				
				e.printStackTrace();
			}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		//declarer.declare(new Fields(EntityConstants.ENTITY,EntityConstants.LOSSY_COUNTS,EntityConstants.SENTI_SCORE));
		declarer.declare(new Fields(EntityConstants.OUTPUT));		
		
	}
	
	@Override
	public void cleanup() {

		/*if(_generateFile)
			try {
				
				_fileWriter.flush();
				_fileWriter.close();
			} catch (IOException e) {
				
				e.printStackTrace();
			}*/
		
	}
	
	private void _updateLossyCount(String entity, long score) {
		
		if(_numItemsParsed == _bucketSize) {
			
			System.out.println("Deleting the items current count - " + _items.size());
			_deleteItems();
			
			System.out.println("After deletion of items current count - " + _items.size());
			
			//_emitItems();
			
			System.out.println("Emitted the items current count - " + _items.size());
			
			_currentBucket 	+= 1;
			_numItemsParsed =  0;
		}
		
		System.out.println("_numItemsParsed = " + _numItemsParsed +" ;_bucketSize = " + _bucketSize + "; _currentBucket =  "+ _currentBucket + ": items count = "+_items.size() );
		
		_updateEntityFrequency(entity,score);
				
	}
	
	
	private void _updateEntityFrequency(String entity, long sentiScore) {
		
		if(_items.containsKey(entity)){
			
			EntityFrequency l = _items.get(entity);
			
			//.. increase the frequency if the entity is already in the items
			l.Frequency  += 1;
			l.SentiScore =  (l.SentiScore + sentiScore)/2;
		
			
			//.. update the hash map with new lossy count object
			_items.put(entity, l);
			
		}
		else {
			
			EntityFrequency l = new EntityFrequency();
			
			l.Entity 		= entity;
			l.SentiScore 	= sentiScore;
			l.Frequency		= 1;
			l.Delta			= _currentBucket-1;
			
			_items.put(entity, l);
		}
		
		_numItemsParsed += 1;
		
	}
	
	private void _deleteItems() {
		
		List<String> itemsToDelete = new ArrayList();
		
		for(String key : _items.keySet()) {
			
			EntityFrequency l = _items.get(key);
			
			if(l.Frequency + l.Delta <= _currentBucket) 
				itemsToDelete.add(key);
		}
		
		for(String key : itemsToDelete) {
			
			_items.remove(key);
		}
		
	}
	
	private synchronized void _emitItems(Map<String, EntityFrequency> items) {
		
		for(String key : items.keySet()) {
			
			EntityFrequency l = items.get(key);	
			
			if(l.Frequency >= (this._threshold - this._epsilon ) * _numItemsParsed ) {				
				//this._collector.emit(new Values(key,l.Frequency,l.SentiScore));
				this._collector.emit(new Values(l));
			}
			
			/*if(_generateFile)
				try {
					_fileName 		= "/s/chopin/l/grad/avik/workspace/CS535/Assignment2/Lossy.txt";
					_fileWriter 	= new FileWriter(_fileName,true);
					BufferedWriter _bw 		= new BufferedWriter(_fileWriter);			
										
					_bw.write(l.Entity + ":" + l.SentiScore + ":" + l.Frequency);
					_bw.write("\n");			
					_bw.flush();
					_fileWriter.flush();
					
				} catch (IOException e) {
					System.out.println("Exception raised - " + e.getMessage());
					e.printStackTrace();
				}*/
		}
		
	}
	

}

