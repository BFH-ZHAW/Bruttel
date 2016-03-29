package com.bruttel;

import java.io.FileReader;
import java.io.IOException;

import au.com.bytecode.opencsv.CSVReader;

import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class PreActus {

	public static void main(String[] args) throws IOException {
		
		String inputFile = "/home/cloudera/git/learning-spark/files/favourite_animals.csv";
		
		
		// Create a Java Spark Context.
//		SparkConf conf = new SparkConf().setMaster("local").setAppName(
//				"FistSpark");
//		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// TODO Auto-generated method stub
		readContracts();
		readInterest();
		calculate();
		mapReduce();
//		JavaRDD<String> csvFile1 = sc.textFile(inputFile);
//		JavaPairRDD<String[]> csvData = csvFile1.map(new ParseLine());
//		
//		public static class ParseLine implements Function<String, String[]> {
//			public String[] call(String line) throws Exception {
//			CSVReader reader = new CSVReader(new StringReader(line));
//			return reader.readNext();
//			}
//			}
//			JavaRDD<String> csvFile1 = sc.textFile(inputFile);
//			JavaPairRDD<String[]> csvData = csvFile1.map(new ParseLine());
			
			
//			CSVReader reader = new CSVReader(new FileReader(inputFile));
//		     String [] nextLine;
//		     while ((nextLine = reader.readNext()) != null) {
//		        // nextLine[] is an array of values from the line
//		        System.out.println(nextLine[0] + nextLine[1] + "etc...");
//		     }
		
		List<String> myList = Arrays.asList("element1","element2","element3");
		for (String element : myList) {
		  System.out.println (element);
		}
		     
		     CSVReader reader = new CSVReader(new FileReader(inputFile));
//		     List<String[]>
		     List<String[]> myEntries = reader.readAll();
//		     System.out.println(myEntries.get(1));
//		     
		     myList.forEach(new Consumer<String>() {
		    	   public void accept(String element) {
		    	      System.out.println(element);
		    	   }
		    	});
//		     
		     myList.forEach((String element) -> System.out.println(element));
//		     
//		     
		     
		     
		

	}
	
	public String[] call(String line) throws Exception {
		CSVReader reader = new CSVReader(new StringReader(line));
		return reader.readNext();
		}
	
	
	private static void readContracts() {
		// KontraktdatenCSV einlesen
		try{
		System.out.println("CSV mit Kontrakten einlesen");
		
		} catch (Exception e) {
		e.printStackTrace();
	}
	}
	
	private static void readInterest() {
		// ZinsCSV einlesen
		try{
		System.out.println("CSV mit Zinsesn einlesen");
		
		} catch (Exception e) {
			e.printStackTrace();
	}
	}
	
	private static void calculate() {
		// Daten berechnen
		try{
		System.out.println("Rechnen");
		} catch (Exception e) {
			e.printStackTrace();
	}
	}
	private static void mapReduce() {
		// Daten Mappen
		try{
		System.out.println("Map and Reduce");
		} catch (Exception e) {
			e.printStackTrace();
	}
	}
	

}
