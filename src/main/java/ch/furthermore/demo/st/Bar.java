package ch.furthermore.demo.st;

import java.util.UUID;

import org.springframework.stereotype.Component;

@Component
public class Bar {
	private String id = UUID.randomUUID().toString();
	
	public String getBar() {
		return id;
	}
}
