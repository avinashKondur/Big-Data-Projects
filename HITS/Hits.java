package PA1;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import java.util.*;

public class Hits implements Serializable	 {

	private SparkConf _conf = null;
	private JavaSparkContext _context = null;
	private final String HDFS_LOCATION = "hdfs://jackson:31001" ;
	private  String TTILES_LOCATION ;
	private  String LINKS_LOCATION;

	private JavaPairRDD<Long,String> _rootSet = null;

	private JavaPairRDD<Long,Long> _baseSet = null;
	private JavaPairRDD<Long, Double> _hubs = null;
	private JavaPairRDD<Long, Double> _auths = null;

	private final String OUTPUT_PATH ;

	/*
	 * Constructor for Hits, which initialises the Spark Configuration as well as Java spark Context
	 */
	public Hits(String outputPath) {		
		_conf = new SparkConf().setAppName("Hits");

		//		_conf.set("spark.kryoserializer.buffer.max", "1536m");

		_context = new JavaSparkContext(_conf);

		OUTPUT_PATH = outputPath;
	}

	/*
	 * Prepares the RDD objects from the given data set
	 */
	public void SetLocations(String titlesLocation, String pageLinks) {

		TTILES_LOCATION =  titlesLocation;
		LINKS_LOCATION =  pageLinks;

	}

	/*
	 * Creates a root set where title matches the query
	 */
	public void CreateRootSet( String query) {

		JavaRDD<String> titlesFile = _context.textFile(HDFS_LOCATION + TTILES_LOCATION);

		JavaPairRDD<String,Long> allTitles =  titlesFile.zipWithIndex();

		//.. Write the root set values to the file
		JavaPairRDD<String, Long> titles =  allTitles.filter(s -> {
			String[] words = s._1.trim().split("_");

			for(String word : words){							
				if(word.trim().toLowerCase().equals(query.trim().toLowerCase()))
					return true;

			}	
			return false;
		});

		//titles.saveAsTextFile(HDFS_LOCATION + OUTPUT_PATH + "RootSet" );

		//.. Create root set containing only filtered Titles
		_rootSet = titles.mapToPair(x -> new Tuple2<Long, String>(x._2,x._1)).cache();

		_rootSet.saveAsTextFile(HDFS_LOCATION + OUTPUT_PATH + "RootSet" );
	}

	/*
	 * Creates the base set which matches the titles that are pulled out using CreateRootSet().
	 */
	public void CreateBaseSet() throws Exception {

		if(_rootSet == null)
			throw new Exception("Root set is not created yet. Call CreateRootSet(query) before you create base set ");

		JavaRDD<String> pageLinksFile = _context.textFile(HDFS_LOCATION + LINKS_LOCATION);

		/*
		 * Create links individually with from and To 
		 */
		JavaPairRDD<String,String> links = pageLinksFile.mapToPair(s -> {

			String[] fromTo = s.split(":");
			return new Tuple2<String,String>(fromTo[0].trim(),fromTo[1].trim());
		});

		/*
		 * filter the values based on root set
		 */
		List<Long> pages = _rootSet.keys().collect();

		Broadcast<List<Long>> list =  _context.broadcast(pages);

		_baseSet = links.filter(l -> {

			List<Long> pages1 = list.value();

			if(pages1.contains(Long.parseLong(l._1)))
				return true;

			String[] tos = l._2.trim().split(" ");

			for(String to : tos) {					
				if(!to.isEmpty() && pages1.contains(Long.parseLong(to.trim())))
					return true;
			}

			return false;
		}).	
				flatMapToPair( l -> {

					List<Long> pages1 = list.value();

					Long  key = Long.parseLong(l._1);

					List<Tuple2<Long, Long>> p = new ArrayList();

					if(pages1.contains(Long.parseLong(l._1))) {

						String[] tos = l._2.trim().split(" ");	
						for(String n : tos){					
							p.add(new Tuple2<>(key,Long.parseLong(n)));
						}
					}
					else {

						String[] tos = l._2.trim().split(" ");	
						for(String n : tos){
							if(!n.isEmpty() && pages1.contains(Long.parseLong(n.trim())))
								p.add(new Tuple2<>(key,Long.parseLong(n)));
						}
					}

					return p.iterator();

				});

		/*
		 * Write base set to HDFS.
		 */

		_baseSet.saveAsTextFile(HDFS_LOCATION + OUTPUT_PATH + "BaseSet" );
	}


	/*
	 * Calculate the Hub and Authority scores
	 */
	public void CalculateHubAuthorityScores() {


		// Initialize the Hubs and Authorities
		InitializeHubsAuth();


		boolean isConverged = false;

		while(!isConverged) {

			JavaPairRDD<Long, Double> newAuth = _normalize(_updateAuths(_hubs));

			JavaPairRDD<Long, Double> newHubs = _normalize(_updateHubs(newAuth));

			isConverged = _isConverged(newAuth) || _isConverged(newHubs);
			
			_auths = newAuth;
			_hubs = newHubs;
		}
		
		//.. print the hubs and authority scores to file in descending order
		_hubs.join(_rootSet)
			  .mapToPair(f-> new Tuple2<Double, String>(f._2._1, f._2._2))
			  .sortByKey(false)
			  .mapToPair(f -> new Tuple2<String, Double>(f._2,f._1)).saveAsTextFile(HDFS_LOCATION + OUTPUT_PATH + "HubScores" );

		_auths.join(_rootSet)
		  .mapToPair(f-> new Tuple2<Double, String>(f._2._1, f._2._2))
		  .sortByKey(false)
		  .mapToPair(f -> new Tuple2<String, Double>(f._2,f._1)).saveAsTextFile(HDFS_LOCATION + OUTPUT_PATH + "AuthScores" );
	}
	
	/*
	 * Initializes the Hubs and Authorities of the pages to 1.0
	 */
	private void InitializeHubsAuth() {

		JavaRDD<Long> keys = _baseSet.keys().distinct();;
		JavaRDD<Long> values = _baseSet.values().distinct();
		JavaRDD<Long> nodes = keys.union(values).distinct();
		
		_hubs = nodes.mapToPair( r -> new Tuple2<Long, Double>(r, 1.0));
		_auths = _hubs;

	}

	/*
	 * Updates the Authority values after normalization in next iteration
	 */
	private JavaPairRDD<Long, Double> _updateAuths(JavaPairRDD<Long,Double> hubs) {

		JavaPairRDD<Long, Double> newAuth = _baseSet.join(hubs)
				.mapToPair( x-> new Tuple2<>(x._2._1,x._2._2))
				.reduceByKey((x,y)-> x+y);

		//newAuth.cache();

		return newAuth;
	}

	/*
	 * Updates the Hub value after normalization in next iteration
	 */
	private JavaPairRDD<Long, Double> _updateHubs(JavaPairRDD<Long,Double> auth){

		JavaPairRDD<Long, Double> newHubs = _baseSet.mapToPair(f -> new Tuple2<Long,Long>(f._2,f._1))
				.join(auth)
				.mapToPair(f -> new Tuple2<Long,Double>(f._2._1,f._2._2))
				.reduceByKey((x,y)-> x+y);

		return newHubs;

	}

	/*
	 * Normalizes the hub and authority scores.
	 */
	private JavaPairRDD<Long,Double> _normalize(JavaPairRDD<Long,Double> scores) {

		//.. Calculate the sum
		Double sum = scores.mapToPair(f -> new Tuple2<String, Double>("Count",f._2)).reduceByKey((x,y) -> x+y).collect().get(0)._2;

		JavaPairRDD<Long,Double> newScores = scores.mapToPair(f ->new Tuple2<Long, Double>(f._1, f._2/sum));

		return newScores;
	}
	
	private boolean _isConverged(JavaPairRDD<Long,Double> scores ) {
		
		JavaPairRDD<Long, Double > convergedScores = scores.join(_hubs).filter(f ->{
			
			if(Math.abs(f._2._1 - f._2._2) > 0.01)
				return true;
			return false;
			
		}).mapToPair(f -> new Tuple2<>(f._1,f._2._1));
		
		return convergedScores.count() < 10;
		
	}

	public void Dispose() {

		_context.close(); 
		
	}
}
