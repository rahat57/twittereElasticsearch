# twittereElasticsearch
using twitter4j library and vertx framework  i have make to services res srvices first is search which take 1 paramteter
keyword to be search from tweets using twitter4j api and then 2nd service which is indexd tweets used to search tweets until 
the limits going to complete which return a list of almost  round about 18000 tweets data then a function chunks is used to 
divide the list and return list of sublist containing 1000 tweets  then index the tweets in to elastic search which 1.3.3 version 
