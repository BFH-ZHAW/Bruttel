package com.bruttel;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Random;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;


public class CreateFileS3 {
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
				    System.out.println("No Number, therefore 20");
				   amount = 20;				
			}
			
			BasicAWSCredentials awsCreds = new BasicAWSCredentials("AKIAIWR365QOHJUXE7FA", "CSneguMGE33DO+aUxx9NVnIk1G9NnlpIkbzpR7Mm");
			AmazonS3 s3Client = new AmazonS3Client(awsCreds);    
			   
		       try {
		    	   System.out.println("Uploading a new object to S3 from a file\n");
		            s3Client.putObject(new PutObjectRequest("bfh-zhaw", "output/"+path+".csv", createSampleFile(path, amount)));

		    	   
		       } catch (AmazonServiceException ase) {
		            System.out.println("Caught an AmazonServiceException, which means your request made it "
		                    + "to Amazon S3, but was rejected with an error response for some reason.");
		            System.out.println("Error Message:    " + ase.getMessage());
		            System.out.println("HTTP Status Code: " + ase.getStatusCode());
		            System.out.println("AWS Error Code:   " + ase.getErrorCode());
		            System.out.println("Error Type:       " + ase.getErrorType());
		            System.out.println("Request ID:       " + ase.getRequestId());
		        } catch (AmazonClientException ace) {
		            System.out.println("Caught an AmazonClientException, which means the client encountered "
		                    + "a serious internal problem while trying to communicate with S3, "
		                    + "such as not being able to access the network.");
		            System.out.println("Error Message: " + ace.getMessage());
		        }
		       
			
			File file = new File(path);   
		    if ( file.exists()) {
		    	 file.delete();
		    }
		    
		 
			    

			
		
		  
	  }

private static File createSampleFile(String path, int amount) throws IOException {
	//Create temporary file
	File file = File.createTempFile(path, ".csv");
    file.deleteOnExit();
    
    // Ab hier beginnt die Zeitmessung:
    long startTime = System.currentTimeMillis();
    
    
	// ... Write Header to output text file.
    Writer writer = new OutputStreamWriter(new FileOutputStream(file));
	writer.write("Nr,Portfolio,Nennwert,Laufzeit,Zins");
	writer.write("\n"); //newLine();
	
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
			writer.write("\n"); //newLine();
		}
		j = j + 10000;
	}
	writer.close();
	
	//Ende der Zeitmessung:
	long endTime = System.currentTimeMillis();
	
	
	System.out.println("File mit "+j+10000+" Zeilen erstellt.");
	System.out.println("File Gröse: "+(file.length()/1024/1024)+" MB");
	System.out.println("Dauer: "+((endTime-startTime)/1000)+" sec");
    
    return file;
}

}
