package com.bruttel;

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;

import java.util.Arrays;

public final class ThirdSpark {
	
  public static void main(String[] args) throws Exception {
    
    // Verify args lenght
    if (args.length < 1) {
      System.err.println("Usage: ThirdSpark <file>");
      System.exit(1);
    }
	
    SparkConf sparkConf = new SparkConf().setAppName("ThirdSpark");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    
    JavaRDD<String> lines = ctx.textFile(args[0], 1); 
    JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")));
    JavaPairRDD<String, Integer> counts = words.mapToPair(w -> new Tuple2<String, Integer>(w, 1)).reduceByKey((x, y) -> x + y);
    counts.saveAsTextFile("hdfs://counts.txt");
	
    ctx.stop();
  }
}