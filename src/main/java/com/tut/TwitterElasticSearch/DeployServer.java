package com.tut.TwitterElasticSearch;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import twitter4j.Twitter;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;

public class DeployServer extends AbstractVerticle {

	HttpServer server;
	Router router;
	String host;
	int port;
	twitterApi twitterApi;
	String esHost;
	int esPort;
	String indexName;
	String indexType;
	String indexId;
	TransportClient client;
	IndexResponse response;
	UpdateResponse updateresponse;
	
	public DeployServer() {

		this.host = "localhost";
		this.port = 8383;
		this.twitterApi = new twitterApi();
		this.esHost = "localhost";
		this.esPort = 9300;
	}

	@Override
	public void start() {
		server = vertx.createHttpServer();
		router = Router.router(vertx);
		// Enable Multipart form data parsing
		router.route().handler(BodyHandler.create());
		router.route().handler(
				CorsHandler.create("*").allowedMethod(HttpMethod.GET)
						.allowedMethod(HttpMethod.POST)
						.allowedMethod(HttpMethod.OPTIONS)
						.allowedHeader("Content-Type, Authorization"));
		// register routes
		this.registerHandlers();
		// server.requestHandler(router::accept).listen(port,host);
		server.requestHandler(router::accept).listen(port, host);
	}

	/**
	 * For Registering different Routes
	 */
	public void registerHandlers() {
		router.route(HttpMethod.GET, "/").handler(this::welcomeRoute);
		router.route(HttpMethod.POST, "/search").blockingHandler(this::search);
		router.route(HttpMethod.POST, "/IndexTweets").blockingHandler(this::Indextweets);

	}

	/**
	 * welcome route
	 * 
	 * @param routingContext
	 */
	public void welcomeRoute(RoutingContext routingContext) {
		routingContext.response().end("<h1> Welcome To Route </h1>");
	}

	/**
	 * use to search tweets for given keyword
	 * 
	 * @param routingContext
	 * @throws Exception
	 */
	public void search(RoutingContext routingContext) {
		String response;
		String keyword = (routingContext.request().getParam("keyword") == null) ? "iphone"
				: routingContext.request().getParam("keyword");
		// System.out.println("Parameter: "+keyword);
		try {
			response = new ObjectMapper().writeValueAsString(this.searchTweets(this.getTwitterInstance(), keyword));
			//System.out.println("size: "+response.s);
		//	ArrayList<Map<String,Object>> tweetsList=this.searchTweets(this.getTwitterInstance(), keyword);
		//	indexInES(tweetsList);
			
		} catch (Exception ex) {
			response = "{\"status\": \"error\", 'msg' : " + ex.getMessage()
					+ "}";
		}
		routingContext.response().end(response);

	}
	
	public void Indextweets(RoutingContext routingContext) {
		String response;
		String keyword = (routingContext.request().getParam("keyword") == null) ? "iphone": routingContext.request().getParam("keyword");
		 System.out.println("Parameter: "+keyword);
		try {	
			// getting tweets from search
		ArrayList<Map<String,Object>> tweetsList=this.searchTweets(this.getTwitterInstance(), keyword);
		
		//dividing tweet list into chunks
		LinkedList<ArrayList<Map<String, Object>>> chunks = chunks(tweetsList, 1000);
		
		for (ArrayList<Map<String, Object>> chunk : chunks) {
			
			this.indexInES(tweetsList);
		}
		
		response = "{status : 'success: indexed in tweets'}";
		} catch (Exception ex) {
			response = "{\"status\": \"error\", 'msg' : " + ex.getMessage()
					+ "}";
		}
		routingContext.response().end(response);

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
	//Method to index data required 3 parameters IndexNmae,type and Data
		public  void IndexData(String IndexName,String type,String id,Map data) throws InterruptedException, ExecutionException{
			IndexRequest indexRequest = new IndexRequest(IndexName,type,id)
	        .source(data);
	UpdateRequest updateRequest = new UpdateRequest(IndexName, type, id)
	        .doc(data)
	        .upsert(indexRequest);              
	client.update(updateRequest).get();	
			// response=client.prepareIndex(IndexName,type).setSource(data).execute().actionGet();
			 
		}
	/**
	 * use to search tweets
	 * 
	 * @param keyword
	 * @return
	 * @throws Exception
	 */
	public ArrayList<Map<String, Object>> searchTweets(Twitter twitter,
			String keyword) throws Exception {
		ArrayList<Map<String, Object>> tweets = twitterApi.search(twitter,
				keyword);
		return tweets;

	}
	
	 /**
	   * use to index tweets in ES
	   * 
	   * @param tweets
	   * @throws UnknownHostException
	   */
	  public void indexInES(ArrayList<Map<String, Object>> tweets) throws UnknownHostException {
	    TransportClient client = this.getElasticClient(this.esHost, this.esPort);
	    BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();   
	    int count=0;
	    
	    ArrayList<Map<String, Object>> tweets1=(ArrayList<Map<String, Object>>) tweets.subList(0, 500);
	    ArrayList<Map<String, Object>> tweets2=(ArrayList<Map<String, Object>>) tweets.subList(500, 1000);
	    ArrayList<Map<String, Object>> tweets3=(ArrayList<Map<String, Object>>) tweets.subList(1000, 1500);
	 //   ArrayList<Map<String, Object>> tweets4=(ArrayList<Map<String, Object>>) tweets.subList(0, 1000);
	    
	  
	    for(Map<String, Object> tweet : tweets){
	    	System.out.println("data at index: "+count+" added: "+tweet);
	    bulkRequestBuilder.add(client.prepareUpdate("twitter", "tweets", tweet.get("id").toString()).setDoc(tweet).setUpsert(tweet));
	    count++;
	    }
	    bulkRequestBuilder.execute().actionGet();
	    client.close();
	  }
	
	public TransportClient getElasticClient(String esHost,int esPort){
		TransportClient client = new TransportClient().addTransportAddress(new InetSocketTransportAddress(esHost, esPort));
	    return client;
		
	}

	/**
	 * get instance of twitter api
	 * 
	 * @return twitter4jApi
	 * @throws Exception
	 */
	public Twitter getTwitterInstance() throws Exception {
		return twitterApi.getTwitterInstance();
	}
}
