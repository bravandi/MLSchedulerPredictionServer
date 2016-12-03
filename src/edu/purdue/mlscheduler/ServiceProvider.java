package edu.purdue.mlscheduler;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import edu.purdue.mlscheduler.Classification.Predictions;

public class ServiceProvider {
	public static void main(String[] args) throws SQLException, Exception {
		//http://localhost:81/?volume_request_id=1490&clock=100&algorithm=j48
		
        HttpServer server = HttpServer.create(new InetSocketAddress(81), 0);
        server.createContext("/", new MyHandler());
        server.setExecutor(null); // creates a default executor
        server.start();
    }

    static class MyHandler implements HttpHandler {
    	
    	Map<MachineLearningAlgorithm, Classification> classification_map;
    	
    	public MyHandler() throws Exception, SQLException{
    		this.classification_map = new HashMap<MachineLearningAlgorithm, Classification>();
    	}
    	
    	public Map<String, String> queryToMap(String query){
    	    Map<String, String> result = new HashMap<String, String>();
    	    for (String param : query.split("&")) {
    	        String pair[] = param.split("=");
    	        if (pair.length>1) {
    	            result.put(pair[0], pair[1]);
    	        }else{
    	            result.put(pair[0], "");
    	        }
    	    }
    	    return result;
    	}
    	
    	public Classification get_classification(MachineLearningAlgorithm algorithm) throws Exception{
    		if (this.classification_map.containsKey(algorithm) == false){
    			Classification c = new Classification(algorithm);
    			
    			c.create_models("");
    			
    			this.classification_map.put(algorithm, c);
    		}
    		
    		return this.classification_map.get(algorithm);
    	}
    	
    	public void reset(){
    		this.classification_map = new HashMap<MachineLearningAlgorithm, Classification>();
    		
    		System.gc();
    	}
    	
        @Override
        public void handle(HttpExchange t) throws IOException {
        	String query_string = t.getRequestURI().getQuery();
        	
        	OutputStream os = t.getResponseBody();
        	
        	if (query_string == null){
        		t.sendResponseHeaders(400, 0);
                os.close();
        		return;
        	}
        	
        	Map<String, String> params = queryToMap(t.getRequestURI().getQuery());
        	
        	if(params.containsKey("reset")){
        		this.reset();
        		
        		String response = "done";
        		t.sendResponseHeaders(200, response.length());
                os.write(response.getBytes());
        		return;
        	}
        	
        	if (params.containsKey("volume_request_id") == false || params.containsKey("clock") == false){
        		t.sendResponseHeaders(400, 0);
                os.close();
        		return;
        	}
        	
        	int clock = new Integer(params.get("clock"));
        	BigInteger volume_request_id = new BigInteger(params.get("volume_request_id"));
        	MachineLearningAlgorithm algorithm = MachineLearningAlgorithm.parse(params.get("algorithm")); 
        	
        	LinkedList<Predictions> predictions_list = null;
        	
        	try {
        		predictions_list = this.get_classification(algorithm).predict(clock, volume_request_id);
        		
        		Gson gson = new Gson();
        		
        		String response = gson.toJson(predictions_list);
        		
                t.sendResponseHeaders(200, response.length());            
                
                os.write(response.getBytes());
                os.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
    }
}
