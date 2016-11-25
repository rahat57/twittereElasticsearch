package com.tut.TwitterElasticSearch;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.RateLimitStatus;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class twitterApi {

	QueryResult result;
	;


	/**
	 * use to get twitter instance
	 * 
	 * @return Twiiter Instance
	 */
	public Twitter getTwitterInstance() throws Exception {

		Twitter twitter = null;
		twitter = authorizeUser(twitter);
		return twitter;
	}

	public ArrayList<Map<String, Object>> search(Twitter twitter, String keyword) throws InterruptedException, ExecutionException {

		ArrayList<Map<String, Object>> tweetList = new ArrayList<Map<String, Object>>();

		try {
			Query query = new Query(keyword);
			query.setCount(100);
			QueryResult result;
			Map<String, RateLimitStatus> rateLimitStatus = twitter.getRateLimitStatus("search");
			RateLimitStatus searchTweetsRateLimit = rateLimitStatus.get("/search/tweets");
			int cnt = 0;
			do {
				result = twitter.search(query);
				List<Status> tweets = result.getTweets();
				for (Status tweet : tweets) {
					Map<String, Object> tweetData = new HashMap<String, Object>();
					tweetData.put("id", tweet.getId());
					tweetData.put("Name", tweet.getUser().getName());
					tweetData.put("Text", tweet.getText());
					System.out.println("data at index: "+cnt+" added: "+tweet);
					tweetList.add(tweetData);
					cnt++;	
				}
			}
			
			while (searchTweetsRateLimit.getRemaining()!=0);	
		}
		catch (TwitterException ex) {
		//	te.printStackTrace();
			System.out.println("Failed to search tweets: " + ex.getMessage());
			return tweetList;
			//System.exit(0);
		}
		finally{
			System.out.println("Hurrah");
			System.err.println("size of list : "+tweetList.size());
		}
		twitter = null;
		
		return tweetList;
	}
	
		public static Twitter authorizeUser(Twitter twitter)
			throws TwitterException, IOException {
		String env = System.getenv("TSAK_CONF");
		// Twitter twitter=getTwitterInstance();
		if (twitter == null) {
			File propConfFile = new File(env + File.separator
					+ "twitter.properties");
			if (!propConfFile.exists()) {
				System.out.println("tsak.properties file does not exist in: "
						+ env);
			}
			Properties prop = new Properties();
			InputStream propInstream = new FileInputStream(propConfFile);
			prop.load(propInstream);
			propInstream.close();
			String consumerKey = prop.getProperty("consumerKey");
			String consumerSecret = prop.getProperty("consumerSecret");
			String accessToken = prop.getProperty("accessToken");
			String accessSecret = prop.getProperty("accessSecret");
			// System.out.println("consumerKey: "+consumerKey+" accessSecret: "+accessSecret+" accessToken: "+accessToken+" consumerSecret: "+consumerSecret);
			if (consumerKey == null || consumerSecret == null
					|| accessToken == null || accessSecret == null) {
				System.out.println("some or all keys are missing!");
			}

			twitter = new TwitterFactory(
					(new ConfigurationBuilder().setDebugEnabled(true)
							.setOAuthConsumerKey(consumerKey.trim())
							.setOAuthConsumerSecret(consumerSecret.trim())
							.setOAuthAccessToken(accessToken.trim())
							.setOAuthAccessTokenSecret(accessSecret.trim()))
							.build()).getInstance();
		}
		twitter.verifyCredentials();
		return twitter;
	}

}
