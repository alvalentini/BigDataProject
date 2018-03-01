package it.unitn.spark.project.bigdata2018;

import java.io.Serializable;
import java.util.List;

import scala.Tuple2;

public class UserProfile implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private static boolean printTdIdf = false;

	private String user;
	
	private List<Tuple2<String, Double>> features;

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public List<Tuple2<String, Double>> getFeatures() {
		return features;
	}

	public void setFeatures(List<Tuple2<String, Double>> features) {
		this.features = features;
	}

	@Override
	public String toString() {
		String str = user + " -> ";
		boolean first = true;
		for (Tuple2<String, Double> f : features) {
			if (first) {
				first = false;
			}
			else {
				str += ", ";
			}
			if (printTdIdf) {
				str += "(" + f._1 + ", " + String.format( "%.3f", f._2) + ")";
			}
			else {
				str += f._1;
			}
		}
		return str;
	}
}
