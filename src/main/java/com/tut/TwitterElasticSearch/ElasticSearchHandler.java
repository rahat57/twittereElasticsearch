package com.tut.TwitterElasticSearch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.swing.RepaintManager;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;

public class ElasticSearchHandler {
	static TransportClient client;
	static IndexResponse response;
	static UpdateResponse updateresponse;
	static Settings clientSetting;
	static BulkProcessor bulkProcessor;
	static BufferedReader reader;

	public static void main(String[] args) throws IOException {

		// load file
	//	 reader = loadFile("tweets.txt");

		// make ElasticSearch Connection
		client = getElasticSerchConn();
		String line = "";
//		List<String> twitter=new ArrayList<String>();
//		Map<String, Object> tweet=new HashMap<String, Object>();
//		int c = 0;
//		 while((line=reader.readLine())!=null){
//			 twitter.add(line);
			// tweet.put("tweets", line);
//		c++;
	//	tweet.put(line.getKey(),i);
	//	 System.out.println("line :"+c+" data: "+line);
//		 }
		 

		 
//		 IndexData("", "", line);
		// System.exit(0);
		// Searching queries
		// String query = "22 | 39";
	
		SearchResponse response =

		// BoostQuery match one as + and not match as -
		// getBoostQuery("json", "test", "age", "gender", "32", "male");

		// search using AndQuery against two records marching,parameters 6
		// index,type,field1,field2,querymatch1,querymatch2
		// getAndQuery("json","test", "age", "gender", "32", "female");

		// MultiMatch Query used to Search query from more than 1 field
		// getMultiMatchQuery("json","test","name","gender",query);

		// getCommonTermQuery("json","test","gender",query);
		// getSimpleQueryString("json","test","age",query);

		// QueryString is used to search specific criteria ,4
		// parameters,index,type,field,searchquery
		// getQueryString("twitter","tweets","text","Rahat");

		// General search method used to search against index and type

			getSearchResponse("test", "dumy");

		// Range Search Query,5 parameters index,type,field,Lowlimit,HighLimit
		// getRangeQuerySearch("json", "test", "age", 20, 30);

		// getAggregationFilter("twitter", "type");
		// System.err.println("whole response :" + response);

		int count = 1;
		for (SearchHit hit : response.getHits()) {

			Map map = hit.getSource();
			System.out.println("line: " + count + " data: " + map.toString());
			count++;
		}
		System.out.println(count + " :records founded ");

		// write file
		// writeFile("testdata", "put data file here");

		// to updated response
		// updateResponse(update);

	}


	// Method to index data required 3 parameters IndexNmae,type and Data
	public void IndexData(String IndexName, String type, String id,Map<String, Object> data) throws InterruptedException,
			ExecutionException {
		IndexRequest indexRequest = new IndexRequest(IndexName, type, id)
				.source(data);
		UpdateRequest updateRequest = new UpdateRequest(IndexName, type, id)
				.doc(data).upsert(indexRequest);
		client.update(updateRequest).get();
		 response=client.prepareIndex(IndexName,type).setSource(data).execute().actionGet();

	}

	// Method to index data required 3 parameters IndexNmae,type and Data
	public static void IndexData(String IndexName, String type, String data) {

		response = client.prepareIndex(IndexName, type).setSource(data)
				.execute().actionGet();

	}

	// SEARCH USING BOOST QUERY
	public static SearchResponse getBoostQuery(String index, String type,
			String field1, String field2, String queryMatch,
			String querynotmatch) {
		QueryBuilder qb = QueryBuilders.boostingQuery()
				.positive(new TermQueryBuilder(field1, queryMatch))
				.negative(new TermQueryBuilder(field2, querynotmatch))
				.negativeBoost(0.1f);
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch()
				.setIndices(index).setTypes(type).setQuery(qb);
		SearchResponse response = searchRequestBuilder.execute().actionGet();
		return response;
	}

	// SEARCH USING MultiMatch QUERY
	public static SearchResponse getMultiMatchQuery(String index, String type,
			String field1, String field2, String query) {
		QueryBuilder qb = QueryBuilders.multiMatchQuery(query, field1, field2);
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch()
				.setIndices(index).setTypes(type).setQuery(qb)
		// .addHighlightedField(field)
		;
		SearchResponse response = searchRequestBuilder.execute().actionGet();
		return response;
	}

	// SEARCH USING Common Term QUERY
	public static SearchResponse getCommonTermQuery(String index, String type,
			String field, String query) {
		QueryBuilder qb = QueryBuilders.commonTerms(field, query);
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch()
				.setIndices(index).setTypes(type).setQuery(qb)
				.addHighlightedField(field);
		SearchResponse response = searchRequestBuilder.execute().actionGet();
		return response;
	}

	// Search using AND QUERY
	public static SearchResponse getAndQuery(String index, String type,
			String field1, String field2, String match, String match2) {
		QueryBuilder qb = QueryBuilders.boolQuery()
				.must(new TermQueryBuilder(field1, match))
				.must(new TermQueryBuilder(field2, match2));
		// .mustNot(new TermQueryBuilder("content", "test2"))
		// .should(new TermQueryBuilder(field1, match))
		// .should(new TermQueryBuilder(field2, match2))
		;
		SearchRequestBuilder searchRequestbuilder = client.prepareSearch()
				.setIndices(index).setTypes(type).setQuery(qb);
		SearchResponse response = searchRequestbuilder.execute().actionGet();
		return response;
	}

	// SEARCH USING SIMPLE QUERY STRING
	public static SearchResponse getSimpleQueryString(String index,
			String type, String field, String query) {
		QueryBuilder qb = QueryBuilders.simpleQueryString(query).field(field);
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch()
				.setIndices(index).setTypes(type).setQuery(qb)
				.addHighlightedField(field);
		SearchResponse response = searchRequestBuilder.execute().actionGet();
		return response;
	}

	// SEARCH USING QUERY STRING
	public static SearchResponse getQueryString(String index, String type,
			String field, String query) {
		QueryBuilder qb = QueryBuilders.queryString(query).defaultField(field);
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch()
				.setIndices(index).setTypes(type).setQuery(qb)
				.addHighlightedField(field);
		SearchResponse response = searchRequestBuilder.execute().actionGet();
		return response;
	}

	// FUNTION TO SEARCH DATA USING RANGE Query
	public static SearchResponse getRangeQuerySearch(String index, String type,
			String field, int Llimit, int Hlimit) {
		RangeQueryBuilder qb = QueryBuilders.rangeQuery(field).from(Llimit)
				.to(Hlimit).includeLower(true).includeUpper(false);
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch()
				.setIndices(index).setTypes(type)
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(qb)
				.addHighlightedField(field);
		SearchResponse response = searchRequestBuilder.execute().actionGet();

		return response;
	}

	// GENERAL FUNCTION TO SEARCH
	public static SearchResponse getSearchResponse(String index, String type) {
		SearchResponse response = client.prepareSearch(index).setTypes(type)
				.setSearchType(SearchType.QUERY_THEN_FETCH)
			  .setFrom(0).setSize(500)
				// .setFetchSource(new String[] { "" }, null)
				.execute().actionGet();
		return response;
	}

	// SEARCH USING AGGREGATION FILTER
	public static SearchResponse getAggregationFilter(String index, String type) {
		SearchResponse response = client
				.prepareSearch(index, type)
				.setQuery(QueryBuilders.matchAllQuery())
				.addAggregation(
						AggregationBuilders.terms("createdAt").field("id"))
				.execute().actionGet()
		// .addAggregation(
		// AggregationBuilders.dateHistogram("agg2")
		// .field("birth")
		// .interval(DateHistogramInterval.YEAR))
		;
		return response;
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
	public static void writeFile(String fileName, String data)
			throws IOException {
		FileWriter fileWriter = new FileWriter(new File(fileName));
		fileWriter.write(data);
		fileWriter.flush();
		fileWriter.close();
	}

	// ELASTICSEARCH CONNECTION TO SERVER
	public static TransportClient getElasticSerchConn()
			throws UnknownHostException {
		clientSetting = ImmutableSettings.settingsBuilder()
				.put("cluster.name", "elasticsearch").build();
		client = new TransportClient(clientSetting)
				.addTransportAddress((TransportAddress) new InetSocketTransportAddress(
						InetAddress.getByName("localhost"), 9300));
		return client;
	}

	// FUNCTION TO DELETE RESPONSE
	public static DeleteResponse deleteResponse(String index, String type,
			String id) {

		DeleteResponse response = client.prepareDelete(index, type, id)
				.setRefresh(true).execute().actionGet();
		if (response.isFound()) {
			System.err.println("Deleted host");
		}
		return response;

	}

	// FUNCTION TO UPDATE RESPONSE
	public static void updateResponse(String updateJson) {

		updateresponse = client.prepareUpdate().setIndex("twitter")
				.setType("checking").setId("1").setDoc(updateJson).execute()
				.actionGet();

	}

}
