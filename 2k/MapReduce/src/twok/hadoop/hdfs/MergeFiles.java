package twok.hadoop.hdfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class MergeFiles {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		String dir = "/home/prasad/training/data/mydata";
		File directory = new File(dir);
		BufferedWriter writer = new BufferedWriter(new FileWriter("/home/prasad/training/20news"));
		File[] files = directory.listFiles();
		 for(File file : files)
         {
                 String filepath = file.getPath();
                 @SuppressWarnings("resource")
				BufferedReader br = new BufferedReader(new FileReader(filepath));
                 String line = br.readLine();
                 String content = "";
                 content = file.getName() + "\t";
                 while(line != null)
                 {
                	 content = content + " " + line;
                	 line= br.readLine();
                 }
                 writer.append(content);
         }
		 writer.close();
	}

}