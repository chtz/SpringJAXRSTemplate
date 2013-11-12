package ch.furthermore.demo.st;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.amazonaws.geo.GeoDataManager;
import com.amazonaws.geo.GeoDataManagerConfiguration;
import com.amazonaws.geo.model.GeoPoint;
import com.amazonaws.geo.model.PutPointRequest;
import com.amazonaws.geo.model.QueryRadiusRequest;
import com.amazonaws.geo.model.QueryRadiusResult;
import com.amazonaws.geo.util.GeoJsonMapper;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

public class DynamoGeoService {
	private final GeoDataManager geoDataManager; //FIXME re-evaluate production-readieness of this lib (starts new threads, no pagination support, ...)

	public DynamoGeoService(GeoDataManager geoDataManager) {
		this.geoDataManager = geoDataManager;
		
		createTableIfNotExisting();
		
		waitForTableToBeReady();
	}

	private GeoDataManagerConfiguration config() {
		return geoDataManager.getGeoDataManagerConfiguration();
	}
	
	public List<PointData> getPointsForCategory(String category, String... fetchAttributes) { //FIXME All queries must have a condition on the hash key, and it must be of type EQ
		List<PointData> result = new LinkedList<PointData>();
		Map<String, AttributeValue> lastEvaluatedKey = null;
		do {
			Map<String, Condition> keyConditions = new HashMap<String, Condition>();

			Condition hashKeyCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ)
					.withAttributeValueList(new AttributeValue().withS(category));
			keyConditions.put("category", hashKeyCondition);

			
			QueryRequest queryRequest = new QueryRequest()
				    .withTableName(config().getTableName()).withKeyConditions(keyConditions)
					.withIndexName("ixcategory").withConsistentRead(true)
					.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);

			if (lastEvaluatedKey != null) {
				queryRequest.addExclusiveStartKeyEntry(config().getHashKeyAttributeName(),
						lastEvaluatedKey.get(config().getHashKeyAttributeName()));
			}

			QueryResult queryResult = config().getDynamoDBClient().query(queryRequest);
			for (Map<String, AttributeValue> item : queryResult.getItems()) {
				PointData pd = extractPointData(item, fetchAttributes);
				result.add(pd);
			}

			lastEvaluatedKey = queryResult.getLastEvaluatedKey();

		} while (lastEvaluatedKey != null);
		
		return result;
	}
	
	public List<PointData> getPointsWithinRadius(double latitude, double longitude, double radiusInMeter, String... fetchAttributes) {
		GeoPoint centerPoint = new GeoPoint(latitude, longitude);
		
		List<String> attributesToGet = new ArrayList<String>();
		attributesToGet.add(config().getRangeKeyAttributeName());
		attributesToGet.add(config().getGeoJsonAttributeName());
		for (String fa : fetchAttributes) {
			attributesToGet.add(fa);
		}

		QueryRadiusRequest queryRadiusRequest = new QueryRadiusRequest(centerPoint, radiusInMeter);
		queryRadiusRequest.getQueryRequest().setAttributesToGet(attributesToGet);
		QueryRadiusResult queryRadiusResult = geoDataManager.queryRadius(queryRadiusRequest);

		List<PointData> result = new LinkedList<PointData>();
		for (Map<String, AttributeValue> item : queryRadiusResult.getItem()) {
			PointData pd = extractPointData(item, fetchAttributes);
			result.add(pd);
		}
		
		return result;
	}

	private PointData extractPointData(Map<String, AttributeValue> item,
			String... fetchAttributes) {
		PointData pd = new PointData();
		try {
			String geoJsonString = item.get(config().getGeoJsonAttributeName()).getS();
			GeoPoint geoPoint = GeoJsonMapper.geoPointFromString(geoJsonString);
			
			pd.setLatitude(geoPoint.getLatitude());
			pd.setLongitude(geoPoint.getLongitude());
			pd.setRangeKey(item.get(config().getRangeKeyAttributeName()).getS());
			
			for (String fa : fetchAttributes) {
				pd.getData().put(fa, item.get(fa).getS());
			}
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
		return pd;
	}
	
	public void putPoint(PointData pd) { 
		GeoPoint geoPoint = new GeoPoint(pd.getLatitude(), pd.getLongitude());
		AttributeValue rangeKeyValue = new AttributeValue().withS(pd.getRangeKey());

		PutPointRequest putPointRequest = new PutPointRequest(geoPoint, rangeKeyValue);
		
		for (Entry<String, String> e : pd.getData().entrySet()) {
			AttributeValue value = new AttributeValue().withS(e.getValue());
			putPointRequest.getPutItemRequest().getItem().put(e.getKey(), value);
		}

		geoDataManager.putPoint(putPointRequest);
	}

	private void createTableIfNotExisting() {
		DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(config().getTableName());
		try {
			config().getDynamoDBClient().describeTable(describeTableRequest);
		} catch (ResourceNotFoundException e) {
			createTable();
		}
	}

	private void createTable() {
		CreateTableRequest createTableRequest = /*GeoTableUtil.*/ getCreateTableRequest(config());
		config().getDynamoDBClient().createTable(createTableRequest);
	}
	
	private CreateTableRequest getCreateTableRequest(GeoDataManagerConfiguration config) {
		CreateTableRequest createTableRequest = new CreateTableRequest()
				.withTableName(config.getTableName())
				.withProvisionedThroughput(
						new ProvisionedThroughput().withReadCapacityUnits(10L).withWriteCapacityUnits(5L)) //FIXME review defaults
				.withKeySchema(
						new KeySchemaElement().withKeyType(KeyType.HASH).withAttributeName(
								config.getHashKeyAttributeName()),
						new KeySchemaElement().withKeyType(KeyType.RANGE).withAttributeName(
								config.getRangeKeyAttributeName()))
				.withAttributeDefinitions(
						new AttributeDefinition().withAttributeType(ScalarAttributeType.N).withAttributeName(
								config.getHashKeyAttributeName()),
						new AttributeDefinition().withAttributeType(ScalarAttributeType.S).withAttributeName(
								config.getRangeKeyAttributeName()),
						new AttributeDefinition().withAttributeType(ScalarAttributeType.N).withAttributeName(
								config.getGeohashAttributeName()),
						new AttributeDefinition().withAttributeType(ScalarAttributeType.S).withAttributeName("category")) //FIXME
				.withLocalSecondaryIndexes(
						new LocalSecondaryIndex()
								.withIndexName(config.getGeohashIndexName())
								.withKeySchema(
										new KeySchemaElement().withKeyType(KeyType.HASH).withAttributeName(
												config.getHashKeyAttributeName()),
										new KeySchemaElement().withKeyType(KeyType.RANGE).withAttributeName(
												config.getGeohashAttributeName()))
								.withProjection(new Projection().withProjectionType(ProjectionType.ALL)),
						new LocalSecondaryIndex()
								.withIndexName("ixcategory") //FIXME
								.withKeySchema(
										new KeySchemaElement().withKeyType(KeyType.HASH).withAttributeName(
												config.getHashKeyAttributeName()),
										new KeySchemaElement().withKeyType(KeyType.RANGE).withAttributeName("category"))
								.withProjection(new Projection().withProjectionType(ProjectionType.ALL)) //FIXME read doc
						);

		return createTableRequest;
	}
	
	private void waitForTableToBeReady() {
		DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(config().getTableName());
		DescribeTableResult describeTableResult = config().getDynamoDBClient().describeTable(describeTableRequest);
		while (!describeTableResult.getTable().getTableStatus().equalsIgnoreCase("ACTIVE")) {
			try {
				Thread.sleep(2000); //FIXME no endless retry
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			describeTableResult = config().getDynamoDBClient().describeTable(describeTableRequest);
		}
	}
}
