package ch.furthermore.demo.st;

import java.util.List;
import java.util.UUID;

import javax.annotation.PostConstruct;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Path("/points")
public class PointResource {
	@Autowired
	private DynamoGeoServiceFactory dynamoGeoServiceFactory;
	private DynamoGeoService geoService;
	
	@PostConstruct
	public void init() {
		geoService = dynamoGeoServiceFactory.createDynamoGeoService("fooTableX3");
	}
	
	@POST
    @Path("/test1")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
    public List<PointData> pointsAroundMe(PointData pd) {
		pd.setRangeKey(UUID.randomUUID().toString());
		pd.withKeyValue("category", "catA");
		geoService.putPoint(pd);
		
		return geoService.getPointsWithinRadius(pd.getLatitude(), pd.getLongitude(), 100.);
	}
	
	@POST
    @Path("/test2")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
    public List<PointData> pointsAroundMe2(PointData pd) {
		pd.setRangeKey(UUID.randomUUID().toString());
		pd.withKeyValue("category", "catB");
		geoService.putPoint(pd);
		
		return geoService.getPointsWithinRadius(pd.getLatitude(), pd.getLongitude(), 100.);
	}
	
	@GET
    @Path("/test3")
	@Produces(MediaType.APPLICATION_JSON)
    public List<PointData> pointsOfInterest() {
		return geoService.getPointsForCategory("catA");
	}
}
