package com.bruttel;

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Arrays;

public class SecondSpark {
	 public static void main(String[] args) throws Exception {

	// Create a Java Spark Context.
	SparkConf sparkConf = new SparkConf().setMaster("local").setAppName(
			"FistSpark");
	JavaSparkContext sc = new JavaSparkContext(sparkConf);
	
	JavaRDD<String> lines = sc.textFile("/home/cloudera/git/learning-spark/files/spam.txt");
	JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")));
	JavaPairRDD<String, Integer> counts = words.mapToPair(w -> new Tuple2<String, Integer>(w, 1)).reduceByKey((x, y) -> x + y);

	counts.saveAsTextFile("/home/cloudera/count.txt");
	 
	sc.close();
	}
}