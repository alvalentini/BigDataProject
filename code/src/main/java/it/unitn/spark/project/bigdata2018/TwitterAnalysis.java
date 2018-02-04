package it.unitn.spark.project.bigdata2018;

import it.unitn.spark.project.bigdata2018.Tweet;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;

public class TwitterAnalysis {
	
	static String[] stopwords = {"a", "as", "able", "about", "above", "according", "accordingly", "across", "actually", "after", "afterwards", "again", "against", "aint", "all", "allow", "allows", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "an", "and", "another", "any", "anybody", "anyhow", "anyone", "anything", "anyway", "anyways", "anywhere", "apart", "appear", "appreciate", "appropriate", "are", "arent", "around", "as", "aside", "ask", "asking", "associated", "at", "available", "away", "awfully", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "believe", "below", "beside", "besides", "best", "better", "between", "beyond", "both", "brief", "but", "by", "cmon", "cs", "came", "can", "cant", "cannot", "cant", "cause", "causes", "certain", "certainly", "changes", "clearly", "co", "com", "come", "comes", "concerning", "consequently", "consider", "considering", "contain", "containing", "contains", "corresponding", "could", "couldnt", "course", "currently", "definitely", "described", "despite", "did", "didnt", "different", "do", "does", "doesnt", "doing", "dont", "done", "down", "downwards", "during", "each", "edu", "eg", "eight", "either", "else", "elsewhere", "enough", "entirely", "especially", "et", "etc", "even", "ever", "every", "everybody", "everyone", "everything", "everywhere", "ex", "exactly", "example", "except", "far", "few", "ff", "fifth", "first", "five", "followed", "following", "follows", "for", "former", "formerly", "forth", "four", "from", "further", "furthermore", "get", "gets", "getting", "given", "gives", "go", "goes", "going", "gone", "got", "gotten", "greetings", "had", "hadnt", "happens", "hardly", "has", "hasnt", "have", "havent", "having", "he", "hes", "hello", "help", "hence", "her", "here", "heres", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "hi", "him", "himself", "his", "hither", "hopefully", "how", "howbeit", "however", "i", "id", "ill", "im", "ive", "ie", "if", "ignored", "immediate", "in", "inasmuch", "inc", "indeed", "indicate", "indicated", "indicates", "inner", "insofar", "instead", "into", "inward", "is", "isnt", "it", "itd", "itll", "its", "its", "itself", "just", "keep", "keeps", "kept", "know", "knows", "known", "last", "lately", "later", "latter", "latterly", "least", "less", "lest", "let", "lets", "like", "liked", "likely", "little", "look", "looking", "looks", "ltd", "mainly", "many", "may", "maybe", "me", "mean", "meanwhile", "merely", "might", "more", "moreover", "most", "mostly", "much", "must", "my", "myself", "name", "namely", "nd", "near", "nearly", "necessary", "need", "needs", "neither", "never", "nevertheless", "new", "next", "nine", "no", "nobody", "non", "none", "noone", "nor", "normally", "not", "nothing", "novel", "now", "nowhere", "obviously", "of", "off", "often", "oh", "ok", "okay", "old", "on", "once", "one", "ones", "only", "onto", "or", "other", "others", "otherwise", "ought", "our", "ours", "ourselves", "out", "outside", "over", "overall", "own", "particular", "particularly", "per", "perhaps", "placed", "please", "plus", "possible", "presumably", "probably", "provides", "que", "quite", "qv", "rather", "rd", "re", "really", "reasonably", "regarding", "regardless", "regards", "relatively", "respectively", "right", "said", "same", "saw", "say", "saying", "says", "second", "secondly", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen", "self", "selves", "sensible", "sent", "serious", "seriously", "seven", "several", "shall", "she", "should", "shouldnt", "since", "six", "so", "some", "somebody", "somehow", "someone", "something", "sometime", "sometimes", "somewhat", "somewhere", "soon", "sorry", "specified", "specify", "specifying", "still", "sub", "such", "sup", "sure", "ts", "take", "taken", "tell", "tends", "th", "than", "thank", "thanks", "thanx", "that", "thats", "thats", "the", "their", "theirs", "them", "themselves", "then", "thence", "there", "theres", "thereafter", "thereby", "therefore", "therein", "theres", "thereupon", "these", "they", "theyd", "theyll", "theyre", "theyve", "think", "third", "this", "thorough", "thoroughly", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "took", "toward", "towards", "tried", "tries", "truly", "try", "trying", "twice", "two", "un", "under", "unfortunately", "unless", "unlikely", "until", "unto", "up", "upon", "us", "use", "used", "useful", "uses", "using", "usually", "value", "various", "very", "via", "viz", "vs", "want", "wants", "was", "wasnt", "way", "we", "wed", "well", "were", "weve", "welcome", "well", "went", "were", "werent", "what", "whats", "whatever", "when", "whence", "whenever", "where", "wheres", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whos", "whoever", "whole", "whom", "whose", "why", "will", "willing", "wish", "with", "within", "without", "wont", "wonder", "would", "would", "wouldnt", "yes", "yet", "you", "youd", "youll", "youre", "youve", "your", "yours", "yourself", "yourselves", "zero"};
	static Set<String> stopWordSet = new HashSet<String>(Arrays.asList(stopwords));


	public static void main(String[] args) {
		// SparkSession
		Builder builder = new Builder().appName("Twitter analysis");
		if (new File("/home/").exists()) {
			builder.master("local");
		}
		SparkSession spark = builder.getOrCreate();

		// Obtain JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		
		JavaRDD<Tweet> tweetsRDD = spark.read().textFile("../data/tweets_50k.txt").javaRDD().map(line -> {
			String[] parts = line.split("\t\t");
			Tweet t = new Tweet();
			t.setUser(parts[0]);
			String[] words = parts[1].replaceAll("[^a-zA-Z ]", " ").toLowerCase().split("\\s+");
			List<String> features = new ArrayList<>();
			for (String word : words) {
				if (word.length() > 2 && !stopWordSet.contains(word)) {
					features.add(word);
				}
			}
			t.setFeatures(features);
			return t;
		});

		PrintWriter writer;
		try {
			writer = new PrintWriter("tweets.txt");
			for (Tweet tweet : tweetsRDD.collect()) {
				writer.println(tweet);
			}
			writer.close();
		} catch (FileNotFoundException e) {
			
		}		
		
		System.out.println("Number of tweets: " + tweetsRDD.collect().size());
		
		JavaRDD<String> rdd = sc.textFile("../data/tweets_50k.txt");
		JavaPairRDD<String, List<String>> resRDD = rdd.mapToPair((PairFunction<String, String, List<String>>) line -> {
			String[] parts = line.split("\t\t");
			String user = parts[0];
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
		});
		
		List<Tweet> tweets = new ArrayList<>();
		for (Tuple2<String, List<String>> t : resRDD.collect()) {
			Tweet tweet = new Tweet();
			tweet.setUser(t._1);
			tweet.setFeatures(t._2);
			tweets.add(tweet);
		}
		
		try {
			writer = new PrintWriter("users.txt");
			for (Tweet tweet : tweets) {
				writer.println(tweet);
			}
			writer.close();
		} catch (FileNotFoundException e) {
			
		}
		
		System.out.println("Number of user: " + tweets.size());
	}
}
