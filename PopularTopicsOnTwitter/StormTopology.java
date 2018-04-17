import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;

public abstract class StormTopology {
	
	protected final static String TWITTER_SPOUT 			= "twitter-spout"			;
	protected final static String SENTIMENT_BOLT 			= "sentiment-bolt"			;
	protected final static String HASHTAG_BOLT  			= "hashtag-bolt"			;
	protected final static String NAMED_ENTITY_BOLT			= "namedentity-bolt"		;
	protected final static String HASHTAG_LOSSY				= "hashtag-lossy-bolt"		;
	protected final static String NAMED_ENTITY_LOSSY		= "namedentity-lossy-bolt"	;
	protected final static String HASHTAG_OUTPUT			= "hashtag-output"			;
	protected final static String NAMED_ENTITY_OUTPUT		= "namedentity-output"		;
	
	
	protected static final String consumerKey 			= "aPT9lV5tzosVk7iKd7wzKGDZ3"							;
	protected static final String consumerSecret 		= "Zmqg8cae3eAmJ2xVsRW0sfCdShCK0hT4iCRnqkxhdlrTHa5vuJ"	;
	protected static final String accessToken 			= "926275353051615232-vJWjGOsXrGVNzZDSRWjwzzOIFTgreeO"	;
	protected static final String accessTokenSecret 	= "R8T4lNeTXIJUHQSmSw6rNQEytqwwYSMDAXN81flQ9Y66S"		;
	
	protected String HASHTAG_FILE_PATH			= "//s//chopin//l//grad//avik//workspace//CS535//Assignment2//HashTagLog.txt"		;
	protected String NAMED_ENTITY_FILE_PATH 	= "//s//chopin//l//grad//avik//workspace//CS535//Assignment2//NamedEntityLog.txt"	;
	
	protected String TopologyName;
	
	public abstract void SubmitTopology(String clusterMode, double e ,double threshold) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException;
	
	public void PrintSuccessMessage(String clusterMode, String topologyName) {
		
		System.out.println("Successfully started Topology. Below are the details");
		System.out.println("Topology Name : " + topologyName );
		System.out.println("Cluster Mode  : " + clusterMode );
	}

}
