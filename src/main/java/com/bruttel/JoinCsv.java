/**
 * Illustrates joining two csv files
 */
package com.bruttel;

import java.io.StringReader;
import scala.Tuple2;
import au.com.bytecode.opencsv.CSVReader;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class JoinCsv {


  public static void main(String[] args) throws Exception {
		if (args.length != 3) {
      throw new Exception("Usage BasicJoinCsv sparkMaster csv1 csv2");
		}
    String master = args[0];
    String csv1 = args[1];
    String csv2 = args[2];
    JoinCsv jsv = new JoinCsv();
    jsv.run(master, csv1, csv2);
  }

  public void run(String master, String csv1, String csv2) throws Exception {
		JavaSparkContext sc = new JavaSparkContext(
      master, "joincsv", System.getenv("SPARK_HOME"), System.getenv("JARS"));
	
  for (int i = 0; i < 50; i++) {
      
    //CSV Files einlesen
    JavaPairRDD<String, String> csvData1 = sc.wholeTextFiles(csv1);
    JavaPairRDD<String, String> csvData2 = sc.wholeTextFiles(csv2);
   
    //CSV Files in Arrays pro Zeile
   JavaRDD<String[]> csvArray1 = csvData1.flatMap(w ->  new CSVReader(new StringReader(w._2())).readAll());  
    JavaRDD<String[]> csvArray2 = csvData2.flatMap(w ->  new CSVReader(new StringReader(w._2())).readAll());

//Manual Debug Help    
//    System.out.println("CSV csvData2.take(5): "+csvData2.take(5));
//    System.out.println("csvData2.first()): "+csvData2.first());
//    System.out.println("csvArray2.take(5): "+csvArray2.take(5));
//    System.out.println("Arrays.toString(csvArray2.first()): "+Arrays.toString(csvArray2.first()));
//    System.out.println("csvArray2.first()[0]: "+csvArray2.first()[0]);
    
     //CSV Files mit Key aus Array für Join
    JavaPairRDD<Integer, String[]> csvKeyed1 = csvArray1.mapToPair(A -> new Tuple2<Integer, String[]>(Integer.parseInt((A[0].split(";"))[3]), //Key
    		A[0].split(";")));	//restliche Elemente
    JavaPairRDD<Integer, String[]> csvKeyed2 = csvArray2.mapToPair(A -> new Tuple2<Integer, String[]>(Integer.parseInt((A[0].split(";"))[0]), //Key
    		A[0].split(";")));  //restliche Elemente  

//Manual Debug Help        
//    System.out.println("csvKeyed1: "+csvKeyed1);
//    System.out.println("csvKeyed1.first(): "+csvKeyed1.first());
//    System.out.println("csvKeyed1.first()._1: "+csvKeyed1.first()._1);
//    System.out.println("Arrays.toString(csvKeyed1.first()._2)): "+Arrays.toString(csvKeyed1.first()._2));
//    System.out.println(csvKeyed1.first()[0]);

    //CSV Join beide CSV
    JavaPairRDD<Integer, Tuple2<String[], String[]>> csvJoined = csvKeyed1.join(csvKeyed2);
      
    //CSV Daten verarbeiten
    JavaPairRDD<String, Double> csvCalculated = csvJoined.mapToPair(A -> new Tuple2<String, Double>(A._2()._1[1],  //PortfolioName
    		Double.parseDouble(A._2()._1[2]) //Nennwert
    		* Math.pow(Double.parseDouble(A._2()._1[3]) //Zinswert  
    				, Double.parseDouble(A._2()._2[1])) // Hoch Laufzeit   				
    				));    
    
    //Map Reduce -> Summarizes Values per Portfolio 
    JavaPairRDD<String, Double> csvCounts = csvCalculated.reduceByKey((x, y) -> x + y); 
    
  //Output  
  System.out.println("Durchlauf "+i+": csvCounts.take(4) -> "+csvCounts.take(4));
    
  }
 }
}
