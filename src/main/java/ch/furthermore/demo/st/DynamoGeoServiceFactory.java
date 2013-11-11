package ch.furthermore.demo.st;

import java.util.logging.Logger;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Component;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.geo.GeoDataManager;
import com.amazonaws.geo.GeoDataManagerConfiguration;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;

@Component
public class DynamoGeoServiceFactory {
	private final static Logger logger = Logger.getLogger(DynamoGeoServiceFactory.class.getName());
	
	private AmazonDynamoDBClient ddb;
	
	@PostConstruct
	public void createDynamoClient() {
		String regionName = System.getProperty("PARAM1", "");

		ClientConfiguration clientConfiguration = new ClientConfiguration().withMaxErrorRetry(20);
		
		if ("".equals(regionName)) { //shall use dynamo mock?
			AWSCredentials credentials = new BasicAWSCredentials("fakeAccessKey", "fakeSecretKey");
			ddb = new AmazonDynamoDBClient(credentials, clientConfiguration);
			ddb.setEndpoint("http://localhost:8000");
			
			logger.info("DynamoDB client initialized for localhost mock"); //TODO remove DEBUG code
		}
		else { //shall use instance role credentials and production dynamo?
			ddb = new AmazonDynamoDBClient(clientConfiguration); //expects instance role!
			Region region = Region.getRegion(Regions.fromName(regionName));
			ddb.setRegion(region);
			
			logger.info("DynamoDB client initialized for region: " + region); //TODO remove DEBUG code
		}
	}
	
	public DynamoGeoService createDynamoGeoService(String tableName) {
		GeoDataManagerConfiguration config = new GeoDataManagerConfiguration(ddb, tableName);
		return new DynamoGeoService(new GeoDataManager(config));
	}
}
