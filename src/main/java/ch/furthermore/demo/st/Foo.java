package ch.furthermore.demo.st;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("request")
@Path("/")
public class Foo {
	private String id = UUID.randomUUID().toString();
	
	@Autowired
	private Bar bar;
	
	@GET
    @Path("/index.html")
	@Produces(MediaType.APPLICATION_JSON)
    public List<String> getBars() {
		return Arrays.asList(new String[]{"Hallo", "Welt", id, bar.getBar()});
	}
}
