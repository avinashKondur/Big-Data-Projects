package DataParsing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import AmazonReviews.StanfordSentiment;

public class ParseAmazonData {

	public static void main(String[] args){

		ParseAmazonData parser = new ParseAmazonData();


		File folder = new File("/s/chopin/l/grad/avik/workspace/CS435/TPInput");
		File[] listOfFiles = folder.listFiles();
		
		System.out.println(listOfFiles.length);

		
		int number = Integer.parseInt(args[0]);
		
		System.out.println("***********Reading file = "+listOfFiles[number].getName());
		
		parser.ParseFileData(listOfFiles[number]);
		
		/*for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				
				System.out.println("********************** Parsing file = " + listOfFiles[i].getName()+"********************** ");
				
				
			} 
		}*/

		//parser.ParseFileData("/s/chopin/l/grad/avik/workspace/CS435/TPInput/sample.json");

	}


	private void _writeFileData(String filename, String data){


		try {

			// Assume default encoding.
			FileWriter fileWriter =
					new FileWriter(filename,true);

			// Always wrap FileWriter in BufferedWriter.
			BufferedWriter bufferedWriter =
					new BufferedWriter(fileWriter);

			// Note that write() does not automatically
			// append a newline character.
			bufferedWriter.write(data);
			bufferedWriter.newLine();

			// Always close files.
			bufferedWriter.close();

		}
		catch(IOException ex) {
			System.out.println(
					"Error writing to file '"
							+ filename + "'");
			// Or we could just do this:
			ex.printStackTrace();
		}

	}

	public void ParseFileData(File fileName){

		// This will reference one line at a time
		String line = null;

		//..List<String> _fileData = new ArrayList<String>();

		try {
			// FileReader reads text files in the default encoding.
			FileReader fileReader = 
					new FileReader(fileName);

			// Always wrap FileReader in BufferedReader.
			BufferedReader bufferedReader = 
					new BufferedReader(fileReader);

			StanfordSentiment sa = new StanfordSentiment();			

			while((line = bufferedReader.readLine()) != null) {


				JSONParser parser = new JSONParser();			

				StringBuilder str = new StringBuilder();

				JSONObject obj2;
				try {
					obj2 = (JSONObject)parser.parse(line);

					str.append(obj2.get("reviewerID").toString()).append("\t");
					str.append(obj2.get("asin").toString()).append("\t");
					str.append(obj2.get("overall").toString()).append("\t");

					String reviewText = obj2.get("reviewText").toString();

					//System.out.println("======================= str till now============="+str.toString());

					if(reviewText.isEmpty())
						str.append("#####");
					else
						//	.. Identify the sentiment of the review text
						str.append(sa.IdentifySentiment(reviewText));
					//str.append("#####");

					_writeFileData(fileName.getName().replaceAll(".json", ""),str.toString());

				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();

				}

			}   

			// Always close files.
			bufferedReader.close();         
		}

		catch(FileNotFoundException ex) {
			System.out.println(
					"Unable to open file '" + 
							fileName + "'");                
		}
		catch(IOException ex) {
			System.out.println(
					"Error reading file '" 
							+ fileName + "'");                  
			// Or we could just do this: 
			ex.printStackTrace();
		}

	}

}
