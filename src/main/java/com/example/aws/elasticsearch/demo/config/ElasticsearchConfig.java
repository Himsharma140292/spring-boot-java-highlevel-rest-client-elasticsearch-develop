package com.example.aws.elasticsearch.demo.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class ElasticsearchConfig {

	/*
	  public  TransportClient getClient() {
    //Create Client
      Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "zencubes-search").put("node.name","Darkhawk").build();
      TransportClient transportClient = new TransportClient(settings);
      transportClient.addTransportAddress(new InetSocketTransportAddress(
              "x.x.x.x",9300));
	  
      return transportClient;
	  }
	*/
	
	/*@Value("${elasticsearch.host}")
    private String elasticsearchHost;

    @Bean(destroyMethod = "close")
    public RestHighLevelClient client() {

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(elasticsearchHost)));

        return client;

    }
*/
   @Value("${elasticsearch.host}")
    private String host;

    @Value("${elasticsearch.port}")
    private int port;

    @Value("${elasticsearch.username}")
    private String userName;

    @Value("${elasticsearch.password}")
    private String password;
    
    // search-knoblr-cluster-i6w6j3t2opra5akaxxej6mai64.us-east-2.es.amazonaws.com
    
   /* private static String serviceName = "es";
    private static String region = "us-east-2";
    private static String aesEndpoint = "search-knoblr-cluster-i6w6j3t2opra5akaxxej6mai64.us-east-2.es.amazonaws.com"; // e.g. https://search-mydomain.us-west-1.es.amazonaws.com
    private static String index = "my-index";
    private static String type = "_doc";
    private static String id = "1";
    
    RestHighLevelClient esClient = esClient(serviceName, region);
    
    // Adds the interceptor to the ES REST client
    public static RestHighLevelClient esClient(String serviceName, String region) {
        AWS4Signer signer = new AWS4Signer();
        signer.setServiceName(serviceName);
        signer.setRegionName(region);
        HttpRequestInterceptor interceptor = new AWSRequestSigningApacheInterceptor(serviceName, signer, credentialsProvider);
        return new RestHighLevelClient(RestClient.builder(HttpHost.create(aesEndpoint)).setHttpClientConfigCallback(hacb -> hacb.addInterceptorLast(interceptor)));
    }*/

    @Bean(destroyMethod = "close")
    public RestHighLevelClient restClient() {
    	
    	

        /*final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(userName, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(host, port))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        RestHighLevelClient client = new RestHighLevelClient(builder);

        */
        System.out.println("##################  "+host);
        
        RestHighLevelClient client = new RestHighLevelClient(        		
                RestClient.builder(new HttpHost(host)));
        
        
        System.out.println("##################  "+client);
        return client;

    }


}
