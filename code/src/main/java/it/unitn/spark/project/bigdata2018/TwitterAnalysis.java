package it.unitn.spark.project.bigdata2018;

import it.unitn.spark.project.bigdata2018.UserProfile;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;

public class TwitterAnalysis {
	
	static private String[] stopwords = new StopWordsRemover().getStopWords();
	static private Set<String> stopWordSet = new HashSet<String>(Arrays.asList(stopwords));
	static private int profile_length = 10;

	private static List<String> featuresFromTweet(String tweet, boolean duplicate) {
		String[] words = tweet.replaceAll("@[^\\s]+", " ").replaceAll("((www\\.[^\\s]+)|(https?://[^\\s]+)|(pic\\.twitter\\.com/[^\\s]+))", " ").replaceAll("[^a-zA-Z ]", " ").toLowerCase().split("\\s+");
		List<String> features = new ArrayList<>();
		for (String word : words) {
			word = new PorterStemmer().stem(word);
			if (word.length() > 2 && word.length() < 16 && !stopWordSet.contains(word) &&
				!word.matches(".*(a{3,}|b{3,}|c{3,}|d{3,}|e{3,}|f{3,}|g{3,}|h{3,}|i{3,}|j{3,}|k{3,}|l{3,}|m{3,}|n{3,}|o{3,}|p{3,}|q{3,}|r{3,}|s{3,}|t{3,}|u{3,}|v{3,}|w{3,}|x{3,}|y{3,}|z{3,}).*") &&
				!word.matches("(.*(ahah).*)|(.*(haha).*)") &&
				(duplicate || !features.contains(word))) {
				features.add(word);
			}
		}
		return features;
	}

	public static void main(String[] args) {
		Builder builder = new Builder().appName("Twitter analysis");

		SparkSession spark = builder.getOrCreate();
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<String> rdd = sc.textFile(args[0]);

		double n = rdd.map(s -> 1).reduce((a, b) -> a + b);

		JavaPairRDD<String, Integer> wordCount = rdd.flatMap(line -> {
			String[] parts = line.split("\t\t");
			if (parts.length < 2) {
				return new ArrayList<String>().iterator();
			}
			List<String> features = featuresFromTweet(parts[1], false);
			return features.iterator();
		}).mapToPair(x -> new Tuple2<String, Integer>(x, 1)).reduceByKey((x, y) -> x + y);
		
		Map<String, Double> m = new HashMap<String, Double>();
		for (Tuple2<String, Integer> t : wordCount.collect()) {
			m.put(t._1, (double)t._2/n);
		}

		JavaPairRDD<String, List<Tuple2<String, Double>>> userProfiling = rdd.mapToPair((PairFunction<String, String, List<String>>) line -> {
			String[] parts = line.split("\t\t");
			String user = parts[0];
			if (parts.length < 2) {
				return new Tuple2<>(user, new ArrayList<>());
			}
			List<String> features = featuresFromTweet(parts[1], true);
			return new Tuple2<>(user, features);
		}).reduceByKey((x, y) -> {
			x.addAll(y);
			return x;
		}).mapToPair((PairFunction<Tuple2<String, List<String>>, String, List<Tuple2<String, Double>>>) p -> {
			List<String> words = p._2;
			double l = words.size();
			Map<String, Integer> f = new HashMap<>();
			for (String w : words) {
				if (f.containsKey(w)) {
					Integer old = f.get(w);
					f.put(w, old+1);
				}				
				else {
					f.put(w, 1);
				}
			}
			Map<Double, Set<String>> om = new TreeMap<Double, Set<String>>(new Comparator<Double>() {
			    @Override
			    public int compare(Double a, Double b) {
			        return -a.compareTo(b);
			    }
			});
			for (Map.Entry<String, Integer> e : f.entrySet()) {
				String w1 = e.getKey();
				double idf = Math.log10(1./(double)m.get(w1));
				double tf = (double)e.getValue()/(double)l;
				double tf_idf = tf*idf;
				if (!om.containsKey(tf_idf)) {
					om.put(tf_idf, new HashSet<>());
				}
				om.get(tf_idf).add(w1);
			}
			List<Tuple2<String, Double>> features = new ArrayList<>();
			int i = 0;
			boolean done = false;
			for (Map.Entry<Double, Set<String>> e1 : om.entrySet()) {
				Set<String> w_set = e1.getValue();
				for (String w : w_set) {
					features.add(new Tuple2<>(w, e1.getKey()));
					i++;
					if (i >= profile_length) {
						done = true;
						break;
					}
				}
				if (done) break;
			}
			return new Tuple2<>(p._1, features);			
		});

		List<UserProfile> userProfiles = new ArrayList<>();
		for (Tuple2<String, List<Tuple2<String, Double>>> t : userProfiling.collect()) {
			UserProfile userProfile = new UserProfile();
			userProfile.setUser(t._1);
			userProfile.setFeatures(t._2);
			userProfiles.add(userProfile);
		}

		System.out.println("Number of user: " + userProfiling.collect().size());
		sc.close();

		PrintWriter writer;
		try {
			writer = new PrintWriter("user_profiles.txt");
			for (UserProfile userProfile : userProfiles) {
				if (userProfile.getFeatures().size() > 3) {
					writer.println(userProfile);
				}
			}
			writer.close();
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
	}
}
