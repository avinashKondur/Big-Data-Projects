package PA1;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import scala.Tuple2;


public class App implements Serializable
{
	/**
	 * 
	 * @param args
	 * 
	 * args[0] = file location for Titles
	 * args[1] = file location for page links
	 * args[2] = query
	 * @throws Exception 
	 * 
	 */
    public static void main( String[] args ) throws Exception
    {
    	
    	if(args.length != 4) {
    		
    		System.out.println("No of arguments passed = "+ args.length + "\n Need to provide all the 3 arguments as "
    				+ "<file location for Titles> "
    				+ "<file location for page links> "
    				+ "<query>"
    				+ "<Output Directory>");
    		
    		System.exit(0);
    		
    	}
    		
    	
    	//.. Create a new Hits object
    	Hits hits = new Hits(args[3]);
    	    	
    	hits.SetLocations(args[0], args[1]);
    	
        System.out.println( "***********************************************"+"Creating the Root set" + "*************************************************************" );
        
        hits.CreateRootSet(args[2]);
        
        System.out.println("*************************************************"+"Creating the base set" + "*************************************************************");
        
         hits.CreateBaseSet();
        
        System.out.println("*************************************************"+"Calculating hubs and Authority scores" + "*************************************************************");
        
        hits.CalculateHubAuthorityScores();
        
        hits.Dispose();
    }
}
