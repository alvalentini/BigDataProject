package it.unitn.spark.project.bigdata2018;

import java.io.Serializable;
import java.util.List;

public class Tweet implements Serializable {

	private String user;
	
	private List<String> features;

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public List<String> getFeatures() {
		return features;
	}

	public void setFeatures(List<String> features) {
		this.features = features;
	}

	@Override
	public String toString() {
		return "Tweet [user=" + user + ", features=" + features + "]";
	}
}
