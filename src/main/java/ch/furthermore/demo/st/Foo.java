package ch.furthermore.demo.st;

import java.util.List;
import java.util.UUID;

import javax.annotation.PostConstruct;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Path("/")
public class Foo {
	@Autowired
	private DynamoGeoServiceFactory dynamoGeoServiceFactory;
	private DynamoGeoService geoService;
	
	@PostConstruct
	public void init() {
		geoService = dynamoGeoServiceFactory.createDynamoGeoService("fooTableX");
	}
	
	@GET
    @Path("/index.html")
	@Produces(MediaType.APPLICATION_JSON)
    public List<PointData> getBars() {
		geoService.putPoint(new PointData(47.30177, -122.42512, UUID.randomUUID().toString()));
		
		return geoService.getPointsWithinRadius(47.30177, -122.42512, 100.);
	}
}
