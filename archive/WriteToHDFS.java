package com.bruttel;
import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WriteToHDFS {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
                try{
                        Path pt=new Path("hdfs://160.85.30.40/user/spark/log.txt");
                        FileSystem fs = FileSystem.get(new Configuration());
                        BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
                                                   // TO append data to a file, use fs.append(Path f)
                        String line;
                        line="Disha Dishu Daasha";
                        System.out.println(line);
                        br.write(line);
                        br.close();
                }catch(Exception e){
                        System.out.println("File not found");
                }
        }
}
