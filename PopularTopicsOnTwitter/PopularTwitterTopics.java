import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.utils.Utils;

public class PopularTwitterTopics {
	
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		
		if(args.length != 4 && args.length !=3 ) {
			System.out.println("Usage is jar <parallel/non-parallel> <local/remote> <epsilon> [<threshold: default is 1>]");
			System.exit(0);
		}
	
		
		String topologyName  = null;
		String mode			 = null;
		
		StormTopology topology = null;
		
		switch(args[0]) {
		
			case "parallel":
				topologyName = args[0];
				topology = new ParallelTopology();
				break;
			case "non-parallel":
				topologyName = args[0];
				topology = new NonParallelTopology();
				break;
			default:
				System.out.println("Invalid input for topology. Value should be either parallel or non-parallel");
				System.exit(0);
		}
		
		switch(args[1]) {
		
			case "local":
				 mode	= args[1];
				break;
			case "remote":
				 mode	= args[1];
				break;
			default:
				System.out.println("Invalid input for cluster mode. Value should be either local or remote");
				System.exit(0);
		}
		
		double epsilon = Double.parseDouble(args[2]);
		
		double threshold;
		if(args.length == 4)
			threshold = Double.parseDouble(args[3]);
		else
			threshold = 1;
		
		topology.SubmitTopology(mode, epsilon,threshold);
		
		Utils.sleep(10000);
	}

}
