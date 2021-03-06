package com.bruttel;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;


//JSoup Framework to render text from web-pages
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;


// For the Spark manipulations:
import scala.Tuple2;

public class FirstSpark {

	public static void main(String[] args) throws Exception {
		//String inputFile = args[0];
		String outputFile = args[1];

		// Create a Java Spark Context.
		SparkConf conf = new SparkConf().setMaster("local").setAppName(
				"FistSpark");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Delete Output Directory To prevente errors
		FileSystem hdfs = FileSystem.get(new Configuration());
//		Path homeDir=hdfs.getHomeDirectory();
//		Path newFolderPath= new Path(outputFile);
//		
//		newFolderPath=Path.mergePaths(homeDir, newFolderPath);

		Path newFolderPath = new Path(outputFile);
		//System.out.println("New Folder Path: "+newFolderPath);
		if (hdfs.exists(newFolderPath))		{
			//System.out.println("existiert");
			hdfs.delete(newFolderPath, true); // Delete existing Directory
		}
		//hdfs.mkdirs(newFolderPath);     //Create new Directory
		//System.exit(0);
		
		// Create text files from Wikipedia (It can be done once in a while)
		// readList();

		// Load our input data from String.
		// JavaRDD<String> input = sc.textFile(inputFile);

		// Load input data from Wikipedia Files:
		JavaRDD<String> input = sc.textFile("/home/cloudera/Documents/wiki/*");
		//System.out.println(input+" gelesen");

		// Split up into words.
		JavaRDD<String> words = input
				.flatMap(new FlatMapFunction<String, String>() {
					public Iterable<String> call(String x) {
						//System.out.println("split");
						return Arrays.asList(x.split(" "));
					}
				});
		JavaRDD<String> words2= input.flatMap(f -> Arrays.asList(f.split(" "))); 

		// Transform into word and count.
		JavaPairRDD<String, Integer> counts = words.mapToPair(
				new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String x) {
						//System.out.println("pair");
						return new Tuple2(x, 1);
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer x, Integer y) {
				return x + y;
			}
		});
		
		JavaPairRDD<String, Integer> counts2 = words.mapToPair(s -> new Tuple2<String, Integer>(s, 1)).reduceByKey((x, y) -> x + y);
		
		// Save the word count back out to a text file, causing evaluation.
		//System.out.println("speichern");
	    System.out.println(StringUtils.join(counts.collect(), ","));
		//counts.saveAsTextFile("hdfs://quickstart.cloudera:8020"+outputFile);
		
		//Ziel nur noch ein File speichern!
		//Path newFilePath=new Path(newFolderPath+"/"+counts);
		//System.out.println(newFilePath);
		//hdfs.createNewFile(newFilePath);
		
		//hdfs.create(newFolderPath);

		// Was needed to properly ending the Application
		sc.close();
		System.exit(0);

	}

	private static void readList() {
		try {
			// Wikipedia Liste öffnen laden
			Document doc = Jsoup
					.connect(
							"https://de.wikipedia.org/wiki/Spezial:L%C3%A4ngste_Seiten")
					.get();

			// Alle Listenelemente �ber den entsprechenden Selektor markieren
			// Ein Leerzeichen initiert ein Kindelement des Elternelementes
			// (links)
			// div#hauptseite-ergeignisse => Der DIV mit der ID
			// hauptseite-ereignisse (# => id)
			// div.inhalt => Der DIV mit der Klasse inhalt (. => class)
			// Elements ereignisse =
			// doc.select("div#hauptseite-ereignisse div.inhalt ul li");
			Elements ereignisse = doc.select("div.mw-spcontent li a[href]");

			// Selektierte Elemente ausgeben ohne HTML-Tags
			for (Element e : ereignisse) {
				if (!trim(e.text(), 35).equals("Versionen")) {
					// System.out.println(e.text());
					// print(" %s  (%s)", e.attr("abs:href"), trim(e.text(),
					// 35));
					readUrl(e.attr("abs:href"));
				}

			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void readUrl(String url) {
		try {

			// String url =
			// "https://de.wikipedia.org/wiki/Teletext-Zeichens%C3%A4tze_(ETSI_EN_300_706)";
			// Wikipedia Seite laden
			Document doc = Jsoup.connect(url).get();

			// Gesamten Inhalt auslesen
			Elements ereignisse = doc.select("body");

			// Selektierte Elemente ausgeben ohne HTML-Tags
			for (Element e : ereignisse) {
				try {
					PrintWriter writer = new PrintWriter(
							"/home/cloudera/Documents/wiki/" + replaceHtml(url)
									+ ".txt", "UTF-8");
					writer.println(e.text());
					writer.close();
				} catch (IOException er) {
					er.printStackTrace();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static String replaceHtml(String s) {
		s = s.replace("https://de.wikipedia.org/wiki/", "");
		s = s.replace("/", "");
		return s;
	}

	private static void print(String msg, Object... args) {
		System.out.println(String.format(msg, args));
	}

	private static String trim(String s, int width) {
		if (s.length() > width)
			return s.substring(0, width - 1) + ".";
		else
			return s;
	}

}
