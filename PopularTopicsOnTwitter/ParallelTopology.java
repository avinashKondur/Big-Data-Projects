import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class ParallelTopology extends StormTopology {
	
	private static final String NAMED_ENTITY_FILE_PATH_LOSSY 	= "//s//chopin//l//grad//avik//workspace//CS535//Assignment2//NamedEntityParallelLossy.txt";
	private static final String HASHTAG_OUTPUT_LOSSY 			= "//s//chopin//l//grad//avik//workspace//CS535//Assignment2//HashTagLogParallelLossy.txt";
	
	protected  String HASHTAG_FILE_PATH				= "//s//chopin//l//grad//avik//workspace//CS535//Assignment2//HashTagLogParallel.txt";
	protected  String NAMED_ENTITY_FILE_PATH 			= "//s//chopin//l//grad//avik//workspace//CS535//Assignment2//NamedEntityLogParallel.txt";
	
	public String TOPOLOGY_NAME = "parallel-topology"; 
	
	public void SubmitTopology(String clusterMode, double e,double threshold) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		
		TopologyBuilder builder = new TopologyBuilder();
		
		TwitterSpout tSpout = new TwitterSpout(consumerKey,consumerSecret,accessToken,accessTokenSecret);
		builder.setSpout(TWITTER_SPOUT, tSpout);
		
		SentimentBolt sBolt = new SentimentBolt();
		builder.setBolt(SENTIMENT_BOLT, sBolt).shuffleGrouping(TWITTER_SPOUT);
		
		HashTagBolt hBolt = new HashTagBolt(true);
		builder.setBolt(HASHTAG_BOLT, hBolt).shuffleGrouping(SENTIMENT_BOLT);
		
		LossyCalculationBolt hashLossyBolt = new LossyCalculationBolt(e,HASHTAG_OUTPUT_LOSSY,threshold);
		builder.setBolt(HASHTAG_LOSSY, hashLossyBolt,4)
			   .setNumTasks(8)
			   .fieldsGrouping(HASHTAG_BOLT,new Fields(EntityConstants.ENTITY));
		
		OutputBolt hashOutput = new OutputBolt(HASHTAG_FILE_PATH);
		builder.setBolt(HASHTAG_OUTPUT, hashOutput).shuffleGrouping(HASHTAG_LOSSY);
		
		
		System.out.println(HASHTAG_FILE_PATH);
		
		NamedEntitiesBolt namedEntityBolt = new NamedEntitiesBolt();
		builder.setBolt(NAMED_ENTITY_BOLT, namedEntityBolt).shuffleGrouping(SENTIMENT_BOLT);
		
		LossyCalculationBolt namedEntityLossy = new LossyCalculationBolt(e,NAMED_ENTITY_FILE_PATH_LOSSY,threshold);
		builder.setBolt(NAMED_ENTITY_LOSSY, namedEntityLossy,4)
		   	   .setNumTasks(8)		   			   
			   .fieldsGrouping(NAMED_ENTITY_BOLT,new Fields(EntityConstants.ENTITY)); 
		
		OutputBolt namedEntityOutput = new OutputBolt(NAMED_ENTITY_FILE_PATH);
		builder.setBolt(NAMED_ENTITY_OUTPUT, namedEntityOutput).globalGrouping(NAMED_ENTITY_LOSSY);
		
		System.out.println(NAMED_ENTITY_FILE_PATH);
		
		Config config = new Config();
		config.setNumWorkers(4);
		config.setDebug(true);
		
		if(clusterMode == "local") {
			
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
			
		}
		else{
			
			StormSubmitter.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
		}
		
		PrintSuccessMessage(clusterMode, TOPOLOGY_NAME);
		
	}
}

