package com.bruttel;

import java.io.StringReader;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import scala.Tuple2;
import au.com.bytecode.opencsv.CSVReader;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;

public class FourthSpark {

  public static class ParseLine implements FlatMapFunction<Tuple2<String, String>, String[]> {
    public Iterable<String[]> call(Tuple2<String, String> file) throws Exception {
      CSVReader reader = new CSVReader(new StringReader(file._2()));
      return reader.readAll();
    }
  }
  public static class ParseLine2 implements PairFunction<String, Integer, String[]> {
	    public Tuple2<Integer, String[]> call(String line) throws Exception {
	      CSVReader reader = new CSVReader(new StringReader(line));
	      String[] elements = reader.readNext();
	      Integer key = Integer.parseInt(elements[0]);
	      return new Tuple2(key, elements);
	    }
	  }
  public static void main(String[] args) throws Exception {
		if (args.length != 4) {
      throw new Exception("Usage BasicLoadCsv sparkMaster csvInputFile csvOutputFile key");
		}
//		local /home/cloudera/git/learning-spark/files/favourite_animals.csv /home/cloudera/Documents/output.txt spark
    String master = args[0];
    String csvInput = args[1];
    String outputFile = args[2];
    final String key = args[3];

		JavaSparkContext sc = new JavaSparkContext(
      master, "loadwholecsv", System.getenv("SPARK_HOME"), System.getenv("JARS"));
    JavaPairRDD<String, String> csvData = sc.wholeTextFiles(csvInput);
    System.out.print(csvData);
    JavaRDD<String[]> keyedRDDinline = csvData.flatMap(new FlatMapFunction<Tuple2<String, String>, String[]>() {
        public Iterable<String[]> call(Tuple2<String, String> file) throws Exception {
           // CSVReader reader = new CSVReader(new StringReader(file._2()));
            return new CSVReader(new StringReader(file._2())).readAll();
          }
        });
    
    System.out.println("In Line Versuch");
//    System.out.print(Arrays.toString(keyedRDDinline[1]));
    System.out.print(Arrays.toString(keyedRDDinline.first()));
    
    JavaRDD<String[]> keyedRDDJava8 = csvData.flatMap(w ->  new CSVReader(new StringReader(w._2())).readAll());
    
    System.out.println("In Java 8 Versuch");
//  System.out.print(Arrays.toString(keyedRDDinline[1]));
    System.out.print(Arrays.toString(keyedRDDJava8.first()));
    
    
//    JavaRDD<String> lines = ctx.textFile(args[0], 1); 
//    JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")));
//    JavaPairRDD<String, Integer> counts = words.mapToPair(w -> new Tuple2<String, Integer>(w, 1)).reduceByKey((x, y) -> x + y);
//    counts.saveAsTextFile("hdfs://counts.txt");
//	
//    
//    
//    JavaRDD<Object> keyedRDDeight = csvData.flatMap(r -> ((Object) r).StringReader());
//    
//    
//    Iterable<String[]> file = 
//    
//    JavaRDD<String[]> keyedRDDeight = csvData.flatMap(r -> ((CSVReader) r).readAll());
//        
//    
//    RDD<String> errors = csvData.filter(new Function<String, Boolean>() {
//    	public Boolean call(String x) { return x.contains("error"); }
//    	});
//    
//    JavaRDD<String> lines = sc.textFile("hdfs://log.txt")
//    		.filter(s -> s.contains("error"));
//    
//    JavaPairRDD<String, String> errors2 = csvData.filter(s -> ((List<String[]>) s).contains("error"));
//    
//    RDD<String> errors3 = csvData.filter(new Function<String, Boolean>() {
//    	public Boolean call(String x) { return x.contains("error"); }
//    	});
//    
//    
//    JavaRDD<String[]> result =
//      keyedRDD.filter(new Function<String[], Boolean>() {
//          public Boolean call(String[] input) { return input[0].equals(key); }});
//    System.out.println(keyedRDD.toString());
//    System.out.println(keyedRDD);
//    keyedRDD.
//    
//    List<String[]> myEntries = reader.readAll();
////    System.out.println(myEntries.get(1));
//     myEntries.forEach((String[] element) -> System.out.println(Arrays.toString(element)));
//    //if (obj instanceof String[]) {
//        String[] strArray = (String[]) result;
//        System.out.println(Arrays.toString(strArray));
//        // System.out.println(obj);
//    //}
//        Arrays.toString(result)
    
//
//    keyedRDD.saveAsTextFile(outputFile);
    
    
    
	}
}