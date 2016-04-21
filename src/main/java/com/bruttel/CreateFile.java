package com.bruttel;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class CreateFile {
	// Create a BufferedWriter around a FileWriter.
	  public static void main(String[] args) throws IOException {
			// Check if Amount of Arguments is correct. 
		  if (args.length != 2) {
			      throw new IOException("Usage: Path Amount");
					}
		  //Pass Arguments
			    String path = args[0];
			    String amountString = args[1];
			    Integer amount;
			    //Try and Assing Argument 2 as Integer
			    try { amount = Integer.parseInt(amountString);
				} catch (NumberFormatException e) {
				    System.out.println("No Number, therefore 10 000");
				   amount = 10000;				
			}
			File file = new File(path);   
		    if ( file.exists()) {
		    	 file.delete();
		    }
		    
		    // Ab hier beginnt die Zeitmessung:
		    long startTime = System.currentTimeMillis();
			    
			// ... Write to an output text file.
			BufferedWriter writer = new BufferedWriter(new FileWriter( path ));
			// Write Header:Nr. ,Portfolio,Nennwert,Laufzeit,Zins
			writer.write("Nr,Portfolio,Nennwert,Laufzeit,Zins");
			writer.newLine();
			
			// While File is smaller than amount given as parameter in MB (Byte * 1024 = KB *1024 = MB)
			int j = 0; // für den Index
			while ( amount*1024*1024 > file.length()){
				// For Schleife erstellt 10'000 neue Records	
				for(int i= 1; i < 10000; i++){

					writer.write(i+j+","+ //Line ID
								(new Random().nextInt(10)+1)+","+ //Portfolionummern von 1-10
								(new Random().nextInt(1000)+1000)+","+ //Nennwerte von 1000 - 1'000'000
								(new Random().nextInt(5)+1)+","+ //Laufzeit von 1-5 Jahre
								"1."+(new Random().nextInt(9))); //Zins von 1.0 - 1.9 % 
					writer.newLine();
				}
				j = j + 10000;
			}
			writer.close();
			
			//Ende der Zeitmessung:
			long endTime = System.currentTimeMillis();
			
			
			System.out.println("File mit "+j+10000+" Zeilen geschrieben.");
			System.out.println("File Gröse: "+(file.length()/1024/1024)+" MB");
			System.out.println("Dauer: "+((endTime-startTime)/1000)+" sec");
		  
	  }

}
