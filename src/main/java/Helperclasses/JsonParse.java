package Helperclasses;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;


public class JsonParse {

	private JSONArray clientDetails;

	public JSONArray getClientDetails() {
		return clientDetails;
	}

	public void setClientDetails(JSONArray topicDetails) {
		this.clientDetails = topicDetails;
	}
	
	public int parseDetails(String file) {
		
		int successFlag = Constants.FAILURE;
		
		JSONParser parser = new JSONParser();
		JSONObject ob;
		if(new File(file).isFile()) {
		try {
			//ob = (JSONObject)parser.parse(new FileReader("config.json"));
			ob = (JSONObject)parser.parse(new FileReader(file));
			this.clientDetails = (JSONArray) ob.get("subscriberDetails");
			successFlag = Constants.SUCCESS;	
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	   }else {
		   System.out.println("file not exists");
	   }
		return successFlag;
	}

}
