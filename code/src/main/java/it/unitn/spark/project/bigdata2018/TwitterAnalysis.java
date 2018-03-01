package it.unitn.spark.project.bigdata2018;

import it.unitn.spark.project.bigdata2018.UserProfile;
import scala.Tuple2;

import java.io.File;
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
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;

public class TwitterAnalysis {
	
	static private String[] stopwords = {"a", "as", "able", "about", "above", "according", "accordingly", "across", "actually", "after", "afterwards", "again", "against", "aint", "all", "allow", "allows", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "an", "and", "another", "any", "anybody", "anyhow", "anyone", "anything", "anyway", "anyways", "anywhere", "apart", "appear", "appreciate", "appropriate", "are", "arent", "around", "as", "aside", "ask", "asking", "associated", "at", "available", "away", "awfully", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "believe", "below", "beside", "besides", "best", "better", "between", "beyond", "both", "brief", "but", "by", "cmon", "cs", "came", "can", "cant", "cannot", "cant", "cause", "causes", "certain", "certainly", "changes", "clearly", "co", "com", "come", "comes", "concerning", "consequently", "consider", "considering", "contain", "containing", "contains", "corresponding", "could", "couldnt", "course", "currently", "definitely", "described", "despite", "did", "didnt", "different", "do", "does", "doesnt", "doing", "dont", "done", "down", "downwards", "during", "each", "edu", "eg", "eight", "either", "else", "elsewhere", "enough", "entirely", "especially", "et", "etc", "even", "ever", "every", "everybody", "everyone", "everything", "everywhere", "ex", "exactly", "example", "except", "far", "few", "ff", "fifth", "first", "five", "followed", "following", "follows", "for", "former", "formerly", "forth", "four", "from", "further", "furthermore", "get", "gets", "getting", "given", "gives", "go", "goes", "going", "gone", "got", "gotten", "greetings", "had", "hadnt", "happens", "hardly", "has", "hasnt", "have", "havent", "having", "he", "hes", "hello", "help", "hence", "her", "here", "heres", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "hi", "him", "himself", "his", "hither", "hopefully", "how", "howbeit", "however", "http", "https", "i", "id", "ill", "im", "ive", "ie", "if", "ignored", "immediate", "in", "inasmuch", "inc", "indeed", "indicate", "indicated", "indicates", "inner", "insofar", "instead", "into", "inward", "is", "isnt", "it", "itd", "itll", "its", "its", "itself", "just", "keep", "keeps", "kept", "know", "knows", "known", "last", "lately", "later", "latter", "latterly", "least", "less", "lest", "let", "lets", "like", "liked", "likely", "little", "look", "looking", "looks", "ltd", "mainly", "many", "may", "maybe", "me", "mean", "meanwhile", "merely", "might", "more", "moreover", "most", "mostly", "much", "must", "my", "myself", "name", "namely", "nd", "near", "nearly", "necessary", "need", "needs", "neither", "never", "nevertheless", "new", "next", "nine", "no", "nobody", "non", "none", "noone", "nor", "normally", "not", "nothing", "novel", "now", "nowhere", "obviously", "of", "off", "often", "oh", "ok", "okay", "old", "on", "once", "one", "ones", "only", "onto", "or", "other", "others", "otherwise", "ought", "our", "ours", "ourselves", "out", "outside", "over", "overall", "own", "particular", "particularly", "per", "perhaps", "placed", "please", "plus", "possible", "presumably", "probably", "provides", "que", "quite", "qv", "rather", "rd", "re", "really", "reasonably", "regarding", "regardless", "regards", "relatively", "respectively", "right", "said", "same", "saw", "say", "saying", "says", "second", "secondly", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen", "self", "selves", "sensible", "sent", "serious", "seriously", "seven", "several", "shall", "she", "should", "shouldnt", "since", "six", "so", "some", "somebody", "somehow", "someone", "something", "sometime", "sometimes", "somewhat", "somewhere", "soon", "sorry", "specified", "specify", "specifying", "still", "sub", "such", "sup", "sure", "ts", "take", "taken", "tell", "tends", "th", "than", "thank", "thanks", "thanx", "that", "thats", "thats", "the", "their", "theirs", "them", "themselves", "then", "thence", "there", "theres", "thereafter", "thereby", "therefore", "therein", "theres", "thereupon", "these", "they", "theyd", "theyll", "theyre", "theyve", "think", "third", "this", "thorough", "thoroughly", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "took", "toward", "towards", "tried", "tries", "truly", "try", "trying", "twice", "two", "un", "under", "unfortunately", "unless", "unlikely", "until", "unto", "up", "upon", "us", "use", "used", "useful", "uses", "using", "usually", "value", "various", "very", "via", "viz", "vs", "want", "wants", "was", "wasnt", "way", "we", "wed", "well", "were", "weve", "welcome", "well", "went", "were", "werent", "what", "whats", "whatever", "when", "whence", "whenever", "where", "wheres", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whos", "whoever", "whole", "whom", "whose", "why", "will", "willing", "wish", "with", "within", "without", "wont", "wonder", "would", "would", "wouldnt", "yes", "yet", "you", "youd", "youll", "youre", "youve", "your", "yours", "yourself", "yourselves", "zero"};
	static private Set<String> stopWordSet = new HashSet<String>(Arrays.asList(stopwords));
	static private int profile_length = 10;

	public static void main(String[] args) {
		// SparkSession
		Builder builder = new Builder().appName("Twitter analysis");
		if (new File("/home/").exists()) {
			builder.master("local");
		}
		SparkSession spark = builder.getOrCreate();

		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		
		JavaRDD<String> rdd = sc.textFile("../data/tweets_2xuser_A.txt");
		
		double n = rdd.map(s -> 1).reduce((a, b) -> a + b);
		JavaPairRDD<String, Integer> wordCount = rdd.flatMap(line -> {
			String[] parts = line.split("\t\t");
			if (parts.length != 2) {
				List<String> features = new ArrayList<>();
				return features.iterator();
			}
			String[] words = parts[1].replaceAll("[^a-zA-Z ]", " ").toLowerCase().split("\\s+");
			List<String> features = new ArrayList<>();
			for (String word : words) {
				if (word.length() > 2 && !stopWordSet.contains(word) && !features.contains(word)) {
					features.add(word);
				}
			}
			return features.iterator();			
		}).mapToPair(x -> new Tuple2<String, Integer>(x, 1)).reduceByKey((x, y) -> x + y);
		
		Map<String, Double> m = new HashMap<String, Double>();

		for (Tuple2<String, Integer> t : wordCount.collect()) {
			m.put(t._1, (double)t._2/n);
		}
		
		JavaPairRDD<String, List<Tuple2<String, Double>>> userProfiling = rdd.mapToPair((PairFunction<String, String, List<String>>) line -> {
			String[] parts = line.split("\t\t");
			String user = parts[0];
			if (parts.length != 2) {
				List<String> features = new ArrayList<>();
				return new Tuple2<>(user, features);
			}
			String[] words = parts[1].replaceAll("[^a-zA-Z ]", " ").toLowerCase().split("\\s+");
			List<String> features = new ArrayList<>();
			for (String word : words) {
				if (word.length() > 2 && !stopWordSet.contains(word)) {
					features.add(word);
				}
			}
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
				double td = (double)e.getValue()/(double)l;
				double td_idf = td*idf;
				if (!om.containsKey(td_idf)) {
					om.put(td_idf, new HashSet<>());
				}
				om.get(td_idf).add(w1);
			}
			List<Tuple2<String, Double>> features = new ArrayList<>();
			int i = 0;
			for (Map.Entry<Double, Set<String>> e1 : om.entrySet()) {
				Set<String> w_set = e1.getValue();
				for (String w : w_set) {
					features.add(new Tuple2<>(w, e1.getKey()));
					i++;
					if (i >= profile_length) break;
				}
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
		
		sc.close();
		
		PrintWriter writer;
		try {
			writer = new PrintWriter("users.txt");
			for (UserProfile userProfile : userProfiles) {
				writer.println(userProfile);
			}
			writer.close();
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
		
		System.out.println("Number of user: " + userProfiles.size());
	}
}
