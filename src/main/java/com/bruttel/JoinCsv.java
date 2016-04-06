/**
 * Illustrates joining two csv files
 */
package com.bruttel;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import scala.Tuple2;
import au.com.bytecode.opencsv.CSVReader;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function;
//Import factory methods provided by DataTypes.
import org.apache.spark.sql.types.DataTypes;
//Import StructType and StructField
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
//Import Row.
import org.apache.spark.sql.Row;
//Import RowFactory.
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;

public class JoinCsv {

  public static class ParseLine implements PairFunction<String, Integer, String[]> {
    public Tuple2<Integer, String[]> call(String line) throws Exception {
      CSVReader reader = new CSVReader(new StringReader(line));
      String[] elements = reader.readNext();
      Integer key = Integer.parseInt(elements[3]);
      return new Tuple2(key, elements);
    }
  }

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
		
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
    JavaRDD<String> csvFile1 = sc.textFile(csv1);
    JavaRDD<String> csvFile2 = sc.textFile(csv2);
    
    //Original mit ParseLine
    //JavaPairRDD<Integer, String[]> keyedRDD1 = csvFile1.mapToPair(new ParseLine());
    //System.out.print(csvFile1.take(5));
    
    String header = csvFile1.first();
    
    List<StructField> fields = new ArrayList<StructField>();
    for (String fieldName: header.split(";")) {
      fields.add(DataTypes.createStructField(fieldName, DataTypes.IntegerType, true));
    }
    StructType schema = DataTypes.createStructType(fields);
   
    System.out.println(fields);
    
    JavaRDD<String> data = csvFile1.subtract(csvFile1.first());
    
    
    
    
    //ParseLine inline aufgerufen
//    JavaPairRDD<Integer, String[]> keyedRDD1A = csvFile1.mapToPair(new PairFunction<String, Integer, String[]>() {
//        public Tuple2<Integer, String[]> call(String line) throws Exception {
//          CSVReader reader = new CSVReader(new StringReader(line));
//          String[] elements = reader.readNext();
////          Integer key = Integer.parseInt(elements[0]);
//          return new Tuple2(Integer.parseInt(elements[0]), elements);
//        }
//      });
//    ParseLine mit Lambda (Java 1.8)
//    JavaRDD<String[]> keyedRDD1B_1 = csvFile1.flatMap(w ->  new CSVReader(new StringReader(w)).readAll());
//    JavaPairRDD<Integer, String[]> keyedRDD1B_2 = keyedRDD1B_1.mapToPair(f -> new Tuple2(Integer.parseInt(f[3]), f));
//    ParseLine mit Lambda (Java 1.8) in einem String
//    JavaPairRDD<Integer, String[]> keyedRDD1B_3 = csvFile1.mapToPair(f -> new Tuple2(Integer.parseInt(new CSVReader(new StringReader(f)).readNext()[3]),  (new CSVReader(new StringReader(f))))); 

    System.out.println("Inline Versuch");
//  System.out.print(Arrays.toString(keyedRDDinline[1]));
//    System.out.print((keyedRDD1.first()));
//    System.out.print((keyedRDD1A.first()));
//    System.out.print(keyedRDD1B_2.first());
//    System.out.print(keyedRDD1B_3.first());
    
    
//    JavaPairRDD<Integer, String[]> keyedRDD2 = csvFile1.mapToPair(new ParseLine());
//    JavaPairRDD<Integer, Tuple2<String[], String[]>> result = keyedRDD1.join(keyedRDD2);
//    List<Tuple2<Integer, Tuple2<String[], String[]>>> resultCollection = result.collect();
	}
}
