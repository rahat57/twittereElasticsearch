package com.tut.TwitterElasticSearch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.sql.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class Test {
	TransportClient client;
	IndexResponse response;
	String esHost;
	int esPort;
	public Test(){
		this.esHost = "localhost";
		this.esPort = 9300;
	}
	
	public static void main(String[] args) throws IOException {

		ArrayList<Map<String, Object>> tweets = new ArrayList<Map<String, Object>>();
		for (int i = 0; i < 18000; i++) {
			Map<String, Object> tweet = new HashMap<String, Object>();
			tweet.put("id", i);
			tweet.put("name", "Name" + i);
			tweets.add(tweet);

		}

		System.out.println("data added succesfully !");
		
		List<String> tweetdata= new ArrayList<String>();
		LinkedList<ArrayList<Map<String, Object>>> chunks = chunks(tweets, 1000);
		for (ArrayList<Map<String, Object>> chunk : chunks) {
			
			for (Map<String, Object> obj : chunk) {
				
				tweetdata.add(""+obj.get("id"));
				tweetdata.add(""+obj.get("name"));
				System.out.println(" id: "+obj.get("id") +"name "+obj.get("name"));
			}
			
	        //  indexInES(chunk);
	         
	        }
		writeFile("esTest.txt",tweetdata);
		System.out.println("data Indexed succesfully !");

	}
	
	public static LinkedList<ArrayList<Map<String, Object>>> chunks(
			ArrayList<Map<String, Object>> bigList, int n) {
		int partitionSize = n;
		LinkedList<ArrayList<Map<String, Object>>> partitions = new LinkedList<ArrayList<Map<String, Object>>>();
		for (int i = 0; i < bigList.size(); i += partitionSize) {
			 ArrayList<Map<String, Object>>  bulk = new  ArrayList<Map<String, Object>>(bigList.subList(i,
					 Math.min(i + partitionSize, bigList.size())));
			 partitions.add(bulk);
		}

		return partitions;
	}

	public static  void indexInES(ArrayList<Map<String, Object>> tweets)
			throws UnknownHostException {
		TransportClient client = esClient("localhost", 9300);

		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		for (Map<String, Object> tweet : tweets) {
			bulkRequestBuilder.add(client.prepareUpdate("test", "dumy",tweet.get("id").toString()).setDoc(tweet)
					.setUpsert(tweet));
		}
		bulkRequestBuilder.setRefresh(true).execute().actionGet();

		client.close();
	}

	public static  TransportClient esClient(String esHost, int esPort)
			throws UnknownHostException {
		TransportClient client = new TransportClient().addTransportAddress(new InetSocketTransportAddress(esHost,esPort));
		return client;
	}


	// Function to LOAD FILE DATA
	public static BufferedReader loadFile(String file)
			throws FileNotFoundException {

		// CSVReader reader1 = new CSVReader(new FileReader("yourfile.csv"));
		InputStream in = new FileInputStream(new File(file));
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		return reader;
	}

	// FUNCTION TO WRITE DATA
	public static void writeFile(String fileName, List<String> data)
			throws IOException {
		FileWriter fileWriter = new FileWriter(new File(fileName));
		fileWriter.write(data.toString());
		fileWriter.flush();
		fileWriter.close();
	}

}
