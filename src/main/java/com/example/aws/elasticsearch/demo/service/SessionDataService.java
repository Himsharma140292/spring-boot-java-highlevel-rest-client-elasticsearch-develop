package com.example.aws.elasticsearch.demo.service;

import static com.example.aws.elasticsearch.demo.util.Constant.INDEX;
import static com.example.aws.elasticsearch.demo.util.Constant.TYPE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.example.aws.elasticsearch.demo.document.SessionData;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class SessionDataService {

	private RestHighLevelClient client;

	private ObjectMapper objectMapper;

	@Autowired
	public SessionDataService(RestHighLevelClient client, ObjectMapper objectMapper) {
		this.client = client;
		this.objectMapper = objectMapper;

		/*
		 * //Create Client Settings settings =
		 * ImmutableSettings.settingsBuilder().put("cluster.name",
		 * "zencubes-search").put("node.name","Darkhawk").build(); TransportClient
		 * transportClient = new TransportClient(settings);
		 * transportClient.addTransportAddress(new InetSocketTransportAddress(
		 * "x.x.x.x",9300)); return transportClient;
		 */

	}

	public String createSessionData(SessionData sessionData) throws Exception {

		// UUID uuid = UUID.randomUUID();
		// document.setId(uuid.toString());

		IndexRequest indexRequest = new IndexRequest(INDEX, TYPE).source(convertProfileDocumentToMap(sessionData),
				XContentType.JSON);

		/*
		 * IndexRequest indexRequest = new IndexRequest(INDEX, TYPE, document.getId())
		 * .source(convertProfileDocumentToMap(document), XContentType.JSON);
		 */
		IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
		return indexResponse.getResult().name();
	}
	
	public String createElasticIndex(SessionData sessionData) throws Exception {
		
		
    Map<String, Object>  sessionDataMap = convertProfileDocumentToMap(sessionData);
		
	    String fromId = (String) sessionDataMap.get("fromId");
	    String toId = (String) sessionDataMap.get("toId");
	    
	    System.out.println("fromId kkkkkkkkkkkkkkkkkkkkkkkkkkkk : "+fromId);
	    
	    System.out.println("toId kkkkkkkkkkkkkkkkkkkkkkkkkkkk : "+toId);
//---------------------------------------------------------------------------------
		 
	    IndexRequest indexRequestFrom = new IndexRequest(fromId, TYPE).source(convertProfileDocumentToMap(sessionData),
				XContentType.JSON);
	    
	    /*IndexRequest request = new IndexRequest(fromId,"doc");
	        String jsonString = "{" +
	                "\"user\":\"himanshu\"," +
	                "\"postDate\":\"2013-01-30\"," +
	                "\"message\":\"trying out 111111 Elasticsearch\"" +
	                "}";*/
	    
	    IndexResponse indexResponseFrom = null;
	       // IndexResponse response =null;
	        try {
	        	indexResponseFrom = client.index(indexRequestFrom, RequestOptions.DEFAULT);
	        	 //response = client.indexAsync(request,RequestOptions.DEFAULT,null);
	        	//indexResponse = client.index(request, RequestOptions.DEFAULT);
	            System.out.println("kkkkkkkkkkkkkkkkkkkkkkkkkkkk : "+indexResponseFrom);

	        } catch (ElasticsearchException e) {
	            if (e.status() == RestStatus.CONFLICT) {
	            	 System.out.println("CONFLICT kkkkkkkkkkkkkkkkkkkkkkkkkkkk indexResponseFrom : ");
	            }
	        }
	        
	       // client.close();
		//------------------------------------------------------------------------------------------
	        
	        
	        
	        IndexRequest indexRequestTo = new IndexRequest(toId, TYPE).source(convertProfileDocumentToMap(sessionData),
					XContentType.JSON);
		    
		    /*IndexRequest request = new IndexRequest(fromId,"doc");
		        String jsonString = "{" +
		                "\"user\":\"himanshu\"," +
		                "\"postDate\":\"2013-01-30\"," +
		                "\"message\":\"trying out 111111 Elasticsearch\"" +
		                "}";*/
		    
		    IndexResponse indexResponseTo = null;
		       // IndexResponse response =null;
		        try {
		        	indexResponseTo = client.index(indexRequestTo, RequestOptions.DEFAULT);
		        	 //response = client.indexAsync(request,RequestOptions.DEFAULT,null);
		        	//indexResponse = client.index(request, RequestOptions.DEFAULT);
		            System.out.println("kkkkkkkkkkkkkkkkkkkkkkkkkkkk : "+indexResponseTo);

		        } catch (ElasticsearchException e) {
		            if (e.status() == RestStatus.CONFLICT) {
		            	 System.out.println("CONFLICT kkkkkkkkkkkkkkkkkkkkkkkkkkkk indexResponseTo : ");
		            }
		        }
	        /*IndexRequest request1 = new IndexRequest(toId,"doc");
	        String jsonString1 = "{" +
	                "\"user\":\"ashu\"," +
	                "\"postDate\":\"2013-01-30\"," +
	                "\"message\":\"trying out 222222 Elasticsearch\"" +
	                "}";
	        request1.source(jsonString1, XContentType.JSON);
	        IndexResponse response1 =null;
	        try {
	        	
	        	 //response = client.indexAsync(request,RequestOptions.DEFAULT,null);
	            response1 = client.index(request1, RequestOptions.DEFAULT);
	            System.out.println("kkkkkkkkkkkkkkkkkkkkkkkkkkkk : "+response1);

	        } catch (ElasticsearchException e) {
	            if (e.status() == RestStatus.CONFLICT) {
	            }
	        } */
	       System.out.println("indexResponseFrom.getResult().name()  "+indexResponseFrom.getResult().name()); 
	       System.out.println("indexResponseTo.getResult().name()  "+indexResponseTo.getResult().name()); 
	       return indexResponseFrom.getResult().name();
		
	}
	

	public SessionData findSessionDataById(String id) throws Exception {
		System.out.println("id %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% " + id);

		// GetRequest getRequest = new GetRequest(INDEX, TYPE, id);
		GetRequest getRequest = new GetRequest(INDEX, id);
		GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
		Map<String, Object> resultMap = getResponse.getSource();

		return convertMapToProfileDocument(resultMap);

	}
	
	public List<SessionData> findSessionDataByName(String name) throws Exception {
		
		
		SearchRequest searchRequest = buildSearchRequest(INDEX,TYPE);
	        searchRequest.indices(INDEX);
	        searchRequest.types(TYPE);

	        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

	        MatchQueryBuilder matchQueryBuilder = QueryBuilders
	                .matchQuery("fromId",name)
	                .operator(Operator.AND);

	        searchSourceBuilder.query(matchQueryBuilder);

	        searchRequest.source(searchSourceBuilder);

	        SearchResponse searchResponse =
	                client.search(searchRequest, RequestOptions.DEFAULT);

	        return getSearchResult(searchResponse);
		

		//-------------------------------
		
		
		/*SearchRequest searchRequest = buildSearchRequest(INDEX, TYPE);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

		QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.multiMatchQuery(name, "name"));// ("name","Ashutosh"));

		MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("name", name).operator(Operator.AND);

		QueryBuilder qb = QueryBuilders.matchQuery("name", "ashutosh");

		System.out.println("qb :::::::: " + qb);
		searchSourceBuilder.query(matchQueryBuilder);
		searchRequest.source(searchSourceBuilder);

		// -----------------------------------------------------------------------------
		Map<String, Object> map = null;
		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		if (searchResponse.getHits().getTotalHits().value > 0) {
			SearchHit[] searchHit = searchResponse.getHits().getHits();
			for (SearchHit hit : searchHit) {
				map = hit.getSourceAsMap();
				System.out.println("map11111111111:" + Arrays.toString(map.entrySet().toArray()));
			}
		}*/
		// ------------------------------------------------------------------------

		// QueryBuilder queryBuilder = QueryBuilders.matchQuery("name",name);
		// QueryBuilder qb = QueryBuilders.matchQuery("name", "some string");

		// searchSourceBuilder.query(QueryBuilders.nestedQuery("name",queryBuilder,ScoreMode.Avg));

		/*
		 * searchSourceBuilder.query(queryBuilder);
		 * searchRequest.source(searchSourceBuilder);
		 * 
		 * SearchResponse response =
		 * client.search(searchRequest,RequestOptions.DEFAULT);
		 */

		//return getSearchResult(searchResponse);
	}
	
	public List<SessionData> findSessionDataByNameId(String fromId, String toId, String metaData) throws Exception {
		
		
		//from and to id  shoulbe go here 
		//--------------------------------------------------------start
		/*BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		WildcardQueryBuilder wildcardqueryBuilder = QueryBuilders.wildcardQuery("fromId", "*"+fromId+"*");
		WildcardQueryBuilder wildcardqueryBuilder1 = QueryBuilders.wildcardQuery("toId", "*"+toId+"*");
		WildcardQueryBuilder wildcardqueryBuilder2 = QueryBuilders.wildcardQuery("metaData", "*"+metaData+"*");
		
		boolQueryBuilder.must(wildcardqueryBuilder);
		boolQueryBuilder.must(wildcardqueryBuilder1);
		boolQueryBuilder.must(wildcardqueryBuilder2);
		
		SearchRequest searchRequest = new SearchRequest(INDEX);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(boolQueryBuilder);
		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		SearchHit[] searchHits = searchResponse.getHits().getHits();
		System.out.println("findProfileByName ****************444************"+searchHits);
		for (SearchHit hit : searchHits) {
			System.out.println("findProfileByName ****************444************"+searchHits);
		    Map<String, Object> sourceAsMap = hit.getSourceAsMap();
		    System.out.println("findProfileByName ****************444************"+sourceAsMap);
		    String fistName = (String) sourceAsMap.get("fistName");
		    String lastName = (String) sourceAsMap.get("lastName");
		   // int age =   (Integer) sourceAsMap.get("age");
		    //int experienceInYears = (Integer) sourceAsMap.get("experienceInYears");
		    System.out.println("map:333333333333333" + Arrays.toString(sourceAsMap.entrySet().toArray()));
		    System.out.println("fistName " + fistName);
		    System.out.println("lastName " + lastName);
		}*/
		
		//-------------------------------------------------------------------end
		
		SearchRequest searchRequest = new SearchRequest(INDEX);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		
	
		boolQueryBuilder.must(QueryBuilders.matchPhraseQuery("fromId", fromId));
		boolQueryBuilder.must(QueryBuilders.matchPhraseQuery("toId", toId));
		/*boolQueryBuilder.should(QueryBuilders.wildcardQuery("fromId", fromId)).boost(3);
		boolQueryBuilder.should(QueryBuilders.wildcardQuery("toId", toId)).boost(2);*/
		boolQueryBuilder.must(QueryBuilders.wildcardQuery("metaData", "*" + metaData + "*"));
		searchSourceBuilder.query(QueryBuilders.boolQuery().must(boolQueryBuilder));
		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		SearchHit[] searchHits = searchResponse.getHits().getHits();
		for (SearchHit hit : searchHits) {
		    Map<String, Object> sourceAsMap = hit.getSourceAsMap();
		    String fromId1 = (String) sourceAsMap.get("fromId");
		    String toId1 = (String) sourceAsMap.get("toId");
		    String metaData1 = (String) sourceAsMap.get("metaData");
		    System.out.println("fromId1 3333333333333333333333  " + fromId1);
		    System.out.println("toId1 3333333333333333333333 " + toId1);
		    System.out.println("metaData1 3333333333333333333333  " + metaData1);
		}
		
		//////////////////////////wildcard search code shoul go here
		

		/*System.out.println("findProfileByName ****************************");
		SearchRequest searchRequest = new SearchRequest(INDEX);
		System.out.println("findProfileByName **************2222**************");
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		 WildcardQueryBuilder matchQueryBuilder = QueryBuilders.wildcardQuery("fistName", name+"*");
         searchSourceBuilder.query(matchQueryBuilder);
		System.out.println("findProfileByName ***************3333*************");
		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		SearchHit[] searchHits = searchResponse.getHits().getHits();
		System.out.println("findProfileByName ****************444************"+searchHits);
		for (SearchHit hit : searchHits) {
			System.out.println("findProfileByName ****************444************"+searchHits);
		    Map<String, Object> sourceAsMap = hit.getSourceAsMap();
		    System.out.println("findProfileByName ****************444************"+sourceAsMap);
		    String fistName = (String) sourceAsMap.get("fistName");
		    String lastName = (String) sourceAsMap.get("lastName");
		   // int age =   (Integer) sourceAsMap.get("age");
		    //int experienceInYears = (Integer) sourceAsMap.get("experienceInYears");
		    System.out.println("map:333333333333333" + Arrays.toString(sourceAsMap.entrySet().toArray()));
		    System.out.println("fistName " + fistName);
		    System.out.println("lastName " + lastName);
		}
*/
		
		
		
		return getSearchResult(searchResponse);

	}
	
	
        public List<SessionData> findSessionDataByFromId(String fromId, String metaData) throws Exception {
		
		
		//from and to id  shoulbe go here 
		//--------------------------------------------------------start
		/*BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		WildcardQueryBuilder wildcardqueryBuilder = QueryBuilders.wildcardQuery("fromId", "*"+fromId+"*");
		WildcardQueryBuilder wildcardqueryBuilder1 = QueryBuilders.wildcardQuery("toId", "*"+toId+"*");
		WildcardQueryBuilder wildcardqueryBuilder2 = QueryBuilders.wildcardQuery("metaData", "*"+metaData+"*");
		
		boolQueryBuilder.must(wildcardqueryBuilder);
		boolQueryBuilder.must(wildcardqueryBuilder1);
		boolQueryBuilder.must(wildcardqueryBuilder2);
		
		SearchRequest searchRequest = new SearchRequest(INDEX);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(boolQueryBuilder);
		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		SearchHit[] searchHits = searchResponse.getHits().getHits();
		System.out.println("findProfileByName ****************444************"+searchHits);
		for (SearchHit hit : searchHits) {
			System.out.println("findProfileByName ****************444************"+searchHits);
		    Map<String, Object> sourceAsMap = hit.getSourceAsMap();
		    System.out.println("findProfileByName ****************444************"+sourceAsMap);
		    String fistName = (String) sourceAsMap.get("fistName");
		    String lastName = (String) sourceAsMap.get("lastName");
		   // int age =   (Integer) sourceAsMap.get("age");
		    //int experienceInYears = (Integer) sourceAsMap.get("experienceInYears");
		    System.out.println("map:333333333333333" + Arrays.toString(sourceAsMap.entrySet().toArray()));
		    System.out.println("fistName " + fistName);
		    System.out.println("lastName " + lastName);
		}*/
		
		//-------------------------------------------------------------------end
		System.out.println("33333333333333333333333 "+fromId);
		SearchRequest searchRequest = new SearchRequest(fromId);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		
	
		//boolQueryBuilder.must(QueryBuilders.matchPhraseQuery("fromId", fromId));
		//boolQueryBuilder.must(QueryBuilders.matchPhraseQuery("toId", toId));
		/*boolQueryBuilder.should(QueryBuilders.wildcardQuery("fromId", fromId)).boost(3);
		boolQueryBuilder.should(QueryBuilders.wildcardQuery("toId", toId)).boost(2);*/
		boolQueryBuilder.must(QueryBuilders.wildcardQuery("metaData", "*" + metaData + "*"));
		searchSourceBuilder.query(QueryBuilders.boolQuery().must(boolQueryBuilder));
		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		SearchHit[] searchHits = searchResponse.getHits().getHits();
		for (SearchHit hit : searchHits) {
		    Map<String, Object> sourceAsMap = hit.getSourceAsMap();
		    String fromId1 = (String) sourceAsMap.get("fromId");
		    String toId1 = (String) sourceAsMap.get("toId");
		    String metaData1 = (String) sourceAsMap.get("metaData");
		    System.out.println("fromId1 3333333333333333333333  " + fromId1);
		    System.out.println("toId1 3333333333333333333333 " + toId1);
		    System.out.println("metaData1 3333333333333333333333  " + metaData1);
		}
		
		//////////////////////////wildcard search code shoul go here
		

		/*System.out.println("findProfileByName ****************************");
		SearchRequest searchRequest = new SearchRequest(INDEX);
		System.out.println("findProfileByName **************2222**************");
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		 WildcardQueryBuilder matchQueryBuilder = QueryBuilders.wildcardQuery("fistName", name+"*");
         searchSourceBuilder.query(matchQueryBuilder);
		System.out.println("findProfileByName ***************3333*************");
		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		SearchHit[] searchHits = searchResponse.getHits().getHits();
		System.out.println("findProfileByName ****************444************"+searchHits);
		for (SearchHit hit : searchHits) {
			System.out.println("findProfileByName ****************444************"+searchHits);
		    Map<String, Object> sourceAsMap = hit.getSourceAsMap();
		    System.out.println("findProfileByName ****************444************"+sourceAsMap);
		    String fistName = (String) sourceAsMap.get("fistName");
		    String lastName = (String) sourceAsMap.get("lastName");
		   // int age =   (Integer) sourceAsMap.get("age");
		    //int experienceInYears = (Integer) sourceAsMap.get("experienceInYears");
		    System.out.println("map:333333333333333" + Arrays.toString(sourceAsMap.entrySet().toArray()));
		    System.out.println("fistName " + fistName);
		    System.out.println("lastName " + lastName);
		}
*/
		
		
		
		return getSearchResult(searchResponse);

	}

	public List<SessionData> findAllSessionData() throws Exception {

		SearchRequest searchRequest = buildSearchRequest(INDEX, TYPE);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.matchAllQuery());
		searchRequest.source(searchSourceBuilder);

		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		// -----------------------------------------------------------------------------
		Map<String, Object> map = null;
		searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		if (searchResponse.getHits().getTotalHits().value > 0) {
			SearchHit[] searchHit = searchResponse.getHits().getHits();
			for (SearchHit hit : searchHit) {
				map = hit.getSourceAsMap();
				System.out.println("map:" + Arrays.toString(map.entrySet().toArray()));
			}
		}
		// ------------------------------------------------------------------------
		return getSearchResult(searchResponse);
	}

	public String updateSessionData(SessionData document) throws Exception {

		SessionData resultDocument = findSessionDataById(document.metaData);

		UpdateRequest updateRequest = new UpdateRequest(INDEX, TYPE, resultDocument.metaData);

		updateRequest.doc(convertProfileDocumentToMap(document));
		UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);

		return updateResponse.getResult().name();

	}

	public String deleteSessionData(String id) throws Exception {

		DeleteRequest deleteRequest = new DeleteRequest(INDEX, TYPE, id);
		DeleteResponse response = client.delete(deleteRequest, RequestOptions.DEFAULT);

		return response.getResult().name();

	}

	private Map<String, Object> convertProfileDocumentToMap(SessionData profileDocument) {
		return objectMapper.convertValue(profileDocument, Map.class);
	}

	private SessionData convertMapToProfileDocument(Map<String, Object> map) {
		return objectMapper.convertValue(map, SessionData.class);
	}

	private List<SessionData> getSearchResult(SearchResponse response) {

		SearchHit[] searchHit = response.getHits().getHits();

		List<SessionData> profileDocuments = new ArrayList<>();

		for (SearchHit hit : searchHit) {
			profileDocuments.add(objectMapper.convertValue(hit.getSourceAsMap(), SessionData.class));
		}

		return profileDocuments;
	}

	private SearchRequest buildSearchRequest(String index, String type) {

		SearchRequest searchRequest = new SearchRequest();
		searchRequest.indices(index);
		searchRequest.types(type);

		return searchRequest;
	}
}
