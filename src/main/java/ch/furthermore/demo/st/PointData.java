package ch.furthermore.demo.st;

import java.util.HashMap;
import java.util.Map;

public class PointData {
	private double latitude;
	private double longitude;
	private String rangeKey;
	private Map<String, String> data = new HashMap<String, String>();

	public PointData() {}
	
	public PointData(double latitude, double longitude, String rangeKey) {
		this.latitude = latitude;
		this.longitude = longitude;
		this.rangeKey = rangeKey;
	}
	
	public PointData withKeyValue(String key, String value) {
		data.put(key, value);
		
		return this;
	}

	public double getLatitude() {
		return latitude;
	}

	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	public String getRangeKey() {
		return rangeKey;
	}

	public void setRangeKey(String rangeKey) {
		this.rangeKey = rangeKey;
	}

	public Map<String, String> getData() {
		return data;
	}

	public void setData(Map<String, String> data) {
		this.data = data;
	}
}
