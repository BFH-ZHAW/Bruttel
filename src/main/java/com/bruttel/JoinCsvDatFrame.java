/**
 * Illustrates joining two csv files
 */
package com.bruttel;

import org.apache.spark.api.java.JavaSparkContext;
//Import factory methods provided by DataTypes.
import org.apache.spark.sql.types.DataTypes;
//Import StructType and StructField
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;


public class JoinCsvDatFrame {


  public static void main(String[] args) throws Exception {
		if (args.length != 3) {
      throw new Exception("Usage BasicJoinCsv sparkMaster csv1 csv2");
		}
    String master = args[0];
    String csv1 = args[1];
    String csv2 = args[2];
    JoinCsvDatFrame jsv = new JoinCsvDatFrame();
    jsv.run(master, csv1, csv2);
  }

  public void run(String master, String csv1, String csv2) throws Exception {
		
	//Create Spark Context 
	JavaSparkContext sc = new JavaSparkContext(
      master, "joincsv", System.getenv("SPARK_HOME"), System.getenv("JARS"));
		
	//Create SQL Spark Context
	SQLContext sqlContext = new SQLContext(sc);	
	  
	//Create Schema for Portfolio
	StructType schemaPortfolio = new StructType()
			.add("Nr", DataTypes.IntegerType, true)
			.add("Portfolio", DataTypes.DoubleType, true)
			.add("Nennwert", DataTypes.DoubleType, true)
			.add("Laufzeit", DataTypes.IntegerType, true);
	//Create Portfolio Data Frame 	
	DataFrame dfPortfolio = sqlContext.read()
		    .format("com.databricks.spark.csv")
		    .option("inferSchema", "false") //Does not work properly with CSV 
		    .option("header", "true")
		    .schema(schemaPortfolio) //Schema is already created
		    .load(csv1);
	
	//Create SChema for Zins
	StructType schemaZins = new StructType()
			.add("Laufzeit", DataTypes.IntegerType, true)
			.add("Zins", DataTypes.DoubleType, true);
	//Create Zins Data Frame	
	DataFrame dfZins = sqlContext.read()
		    .format("com.databricks.spark.csv")
		    .option("inferSchema", "false")
		    .option("header", "true")
		    .schema(schemaZins)
		    .load(csv2);
//Manual Debug Help
	//Print 20 frist Rows of Data Frame
	dfPortfolio.show();
	dfZins.show();

	// Print the schema in a tree format
	dfPortfolio.printSchema();
	dfZins.printSchema();

	// Select only the "name" column
	dfPortfolio.select("Portfolio").show();


	// Select everybody, but increment the age by 1
	//df.select(df.col("name"), df.col("age").plus(1)).show();

	// Select people older than 21
	//df.filter(df.col("age").gt(21)).show();


	// Count people by age
	//df.groupBy("age").count().show();
	

	}
}
