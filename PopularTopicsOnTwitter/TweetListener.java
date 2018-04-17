import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.CommonTextObjectFactories;
import com.optimaize.langdetect.text.TextObject;
import com.optimaize.langdetect.text.TextObjectFactory;

import twitter4j.StallWarning;
import twitter4j.Status;

import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

public class TweetListener implements StatusListener{

		private LinkedBlockingQueue<Status> _queue ;
		
		private int _queDepth;
		
		private FileWriter _fileWriter ;
		private String _fileName;
		
		public TweetListener(int queDepth) {
			
			this._queDepth = queDepth;
			this._queue = new LinkedBlockingQueue<Status>();
			
			try {
				_fileName			= "/s/chopin/l/grad/avik/workspace/CS535/Assignment2/Spout.txt";
				_fileWriter 			= new FileWriter(_fileName,true);
			} catch (IOException e) {
				
				e.printStackTrace();
			}
			
		}
		
		public TweetListener(int queDepth,String filename) {
			
			this._queDepth = queDepth;
			this._queue = new LinkedBlockingQueue<Status>();
			
			try {
				_fileName				= filename;
				_fileWriter 			= new FileWriter(filename,true);
			} catch (IOException e) {
				
				e.printStackTrace();
			}
			
		}
		
		//.. Returns the elements only if the queue is not empty
		public Status GetNextObjectFromQueue() {
			Status tweet = _queue.poll();
			
			try {
				_fileWriter 			= new FileWriter(_fileName,true);
				BufferedWriter _bw 		= new BufferedWriter(_fileWriter);			
				if(tweet != null) {					
					_bw.write("Got Element ----- "+tweet.getText() );
					_bw.write("\n");
					
					_bw.flush();
				}
				else {
					
					_bw.write("No tuple Added to Queue");
					_bw.write("\n");
					
					_bw.flush();
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			return tweet;
		}
	
		public void onStatus(Status status) {
			
			try {     
				List<LanguageProfile> languageProfiles = new LanguageProfileReader().readAllBuiltIn();    
				
				LanguageDetector languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
																		   .withProfiles(languageProfiles)
																		   .build();
				
				TextObjectFactory textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText(); 
							 
				TextObject textObject = textObjectFactory.forText(status.getText());    
				com.google.common.base.Optional<LdLocale> lang = languageDetector.detect(textObject);   
				
				if(lang.isPresent()) {
					System.out.println(lang.get().getLanguage());
					
					_fileWriter 			= new FileWriter(_fileName,true);
					BufferedWriter _bw 		= new BufferedWriter(_fileWriter);			
					if(status != null) {					
						_bw.write("In Queue ----- " + lang.get().toString() + "------------"+status.getText() );
						_bw.write("\n");
						
						_bw.flush();
					}
					
				}
				
			    if(lang.isPresent() ){
			    
			    	switch(lang.get().getLanguage().toLowerCase().trim()) {
			    	
			    	case "en" :
			    		System.out.println("Inside If - " + lang.get().getLanguage());
				    	
				    	//.. add the status to queue.
						_queue.put(status);	
						
						_fileWriter 			= new FileWriter(_fileName,true);
						BufferedWriter _bw 		= new BufferedWriter(_fileWriter);			
						if(status != null) {					
							_bw.write("In Queue ----- " + lang.get().getLanguage() + "------------"+status.getText() );
							_bw.write("\n");
							
							_bw.flush();
						}
						
						System.out.println(status.getText());
						
				    	System.out.println(lang.get().toString());  
			    		break;
			    	default:
			    		System.out.println("Inside If - but language did not match " + lang.get().getLanguage());
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
			//System.out.println(status.getText());*/
			
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
		
		
	}