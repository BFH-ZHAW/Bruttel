
/**
 * Illustrates ...
 */
package com.bruttel;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
//Import factory methods provided by DataTypes.
import org.apache.spark.sql.types.DataTypes;
//Import StructType and StructField
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

//import org.apache.spark.sql.SQLContext;
import java.util.Iterator;

public class DataFrameCSVKomplett {


  public static void main(String[] args) throws Exception {
		if (args.length != 3) {
      throw new Exception("Usage BasicJoinCsv sparkMaster csv1");
		}
    String csv1 = args[0];
    String activity = args[2];
    DataFrameCSVKomplett jsv = new DataFrameCSVKomplett();
    jsv.run(csv1, activity);
  }

  public void run(String csv1, String activity) throws Exception {
		
	//Create Spark Context
	SparkSession sparkSession = SparkSession
	   		.builder()
	   		.appName("sparkjobs.MapContractsToEventsJob")
	   		.getOrCreate();  
  
	//Create Schema for Portfolio
	StructType schemaPortfolio = new StructType()
			.add("Nr", DataTypes.IntegerType, true)
			.add("Portfolio", DataTypes.IntegerType, true)
			.add("Nennwert", DataTypes.DoubleType, true)
			.add("Laufzeit", DataTypes.DoubleType, true)
			.add("Zins", DataTypes.DoubleType, true);
		
	//Create Portfolio Data Frame from CSV	
	Dataset<Row> dsPortfolio = sparkSession.read()
		    .option("sep", ";")
		    .schema(schemaPortfolio)
		    .csv(csv1)    // Equivalent to format("csv").load("/path/to/directory")
		    .cache();
	
	//Register as table to use sql later on
	dsPortfolio.createOrReplaceTempView("portfolio");

	//SQL abhängig von Activity:
	String sqlquery; 
	if (activity.equals("count")){
		sqlquery = "SELECT count(*) "
				+ "FROM portfolio ";
	}
	else {
		sqlquery = "SELECT Portfolio, SUM(Nennwert * POW (Zins, Laufzeit)) as Istwert "
				+ "FROM portfolio "
				+ "GROUP BY Portfolio";
	}
	
	//Hier werden die Performance Queries erstellt:
    for(int i=1; i<11; i++){
    	//Start der Zeitmessung
    	long startTime = System.currentTimeMillis();	
    	
    	//SQL Query ausführen
    	dsPortfolio.sqlContext().sql(sqlquery);
    	
    	//Ende der Zeitmessung:
    	long endTime = System.currentTimeMillis();	
    	//Log Ausgabe
    	System.out.println("Dauer Durchgang "+i+": "+((endTime-startTime)/1000)+" sec");
    }
		
	SparkSession.clearActiveSession();
    
	}
}