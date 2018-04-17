import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.CommonTextObjectFactories;
import com.optimaize.langdetect.text.TextObject;
import com.optimaize.langdetect.text.TextObjectFactory;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterSpout extends BaseRichSpout {

	private SpoutOutputCollector _collector;
	
	//.. Twitter keys
	private String _consumerKey       ;
	private String _consumerSecret 	  ;
	private String _accessToken 	  ;
	private String _accessTokenSecret ;
	
	private int QUEUE_DEPTH = 1000 ;
	
	private FileWriter _fileWriter ;
	private String _fileName;
	
	private LinkedBlockingQueue<Status> _queue ;
	
	private  TwitterStream _getTwitterStream() {
					  	
		TwitterStream twitterStream = new TwitterStreamFactory(new ConfigurationBuilder().setJSONStoreEnabled(true).build()).getInstance(); 
		
		StatusListener listener = new StatusListener() {

			public void onStatus(Status status) {				

				try {     
					List<LanguageProfile> languageProfiles = new LanguageProfileReader().readAllBuiltIn();    
					
					LanguageDetector languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
																			   .withProfiles(languageProfiles)
																			   .build();
					
					TextObjectFactory textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText(); 
								 
					TextObject textObject = textObjectFactory.forText(status.getText());    
					com.google.common.base.Optional<LdLocale> lang = languageDetector.detect(textObject);   
					
				    if(lang.isPresent() ){
				    
				    	switch(lang.get().getLanguage().toLowerCase().trim()) {
				    	
				    	case "en" :
				    		System.out.println("Inside If - " + lang.get().getLanguage());
					    	
					    	//.. add the status to queue.
							_queue.put(status);	
								
							if(status != null) {
								_fileName 				= "/s/chopin/l/grad/avik/workspace/CS535/Assignment2/Spout.txt";
								_fileWriter 			= new FileWriter(_fileName,true);
								BufferedWriter _bw 		= new BufferedWriter(_fileWriter);		
								
								_bw.write(status.getText() );
								_bw.write("\n");
								_bw.flush();
							}
							
							System.out.println(status.getText());
							
					    	System.out.println(lang.get().toString());  
				    		break;
				    	default:
				    		System.out.println("language did not match " + lang.get().getLanguage());
				    		break;
				    	}
				    	   
				    } 
				     else{
				    	 
				    	 System.out.println("No language matched!");     
				    }
			     } 
				catch (Exception e) {     
					e.printStackTrace(); 
				
				}
				
				//.. add the status to queue.
				/*try {
					_queue.put(status);
				} catch (InterruptedException e) {
					
					e.printStackTrace();
				}	*/
			}


			public void onException(Exception arg0) {
				// TODO Auto-generated method stub
				
			}

			public void onDeletionNotice(StatusDeletionNotice arg0) {
				// TODO Auto-generated method stub
				
			}

			public void onScrubGeo(long arg0, long arg1) {
				// TODO Auto-generated method stub
				
			}

			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub
				
			}

			

			public void onTrackLimitationNotice(int arg0) {
				// TODO Auto-generated method stub
				
			}
			
		};
		
		twitterStream.addListener( listener); 
		 
		twitterStream.setOAuthConsumer(_consumerKey, _consumerSecret); 
		AccessToken token = new AccessToken(_accessToken, _accessTokenSecret); 
		 
		twitterStream.setOAuthAccessToken(token);
		 		
		return twitterStream;
	}
	
	public TwitterSpout(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret) {
		
		this._consumerKey 		= consumerKey       ;
		this._consumerSecret 	= consumerSecret    ;
		this._accessToken 		= accessToken       ;
		this._accessTokenSecret = accessTokenSecret ;		
		
	}
	
	public void nextTuple() {
		
		Status tweet = this._queue.poll();
		
		try {
			_fileName 				= "/s/chopin/l/grad/avik/workspace/CS535/Assignment2/Spout.txt";
			_fileWriter 			= new FileWriter(_fileName,true);
			BufferedWriter _bw 		= new BufferedWriter(_fileWriter);	
			if(tweet!=null)
				_bw.write(tweet.getText());
			else
				_bw.write("No tweet generated ");
			_bw.write("\n");
			
			_bw.flush();
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		if(tweet != null)
			this._collector.emit(new Values(tweet));
		else {
			
			Utils.sleep(50);
			
			try{
				
				Thread.sleep(1000);					
			}
			catch(Exception e)				{
				e.printStackTrace();
			}
		}
	}
	
	public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {

		this._collector 		= collector;
		//this._listener     		= new TweetListener(QUEUE_DEPTH)  ;
		this._queue				= new LinkedBlockingQueue<Status>();
		
		TwitterStream tStream 	= _getTwitterStream();
		
		//.. Create a Filter
		FilterQuery tweetFilterQuery = new FilterQuery(); 
		//tweetFilterQuery.track(new String[] {"#" });
		tweetFilterQuery.language(new String[]{"en"});
		
		//tStream.filter(tweetFilterQuery);
		tStream.sample();
		
		Utils.sleep(10000);
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields(EntityConstants.TWEET));
	}

}
