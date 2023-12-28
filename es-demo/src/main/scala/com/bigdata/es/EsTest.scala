package com.bigdata.es

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.{search, update}
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.{QueryBuilders, TermQueryBuilder}
import org.elasticsearch.index.reindex.UpdateByQueryRequest
import org.elasticsearch.script.{Script, ScriptType}
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms
import org.elasticsearch.search.aggregations.metrics.ParsedAvg
import org.elasticsearch.search.aggregations.{Aggregation, AggregationBuilders, BucketOrder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder
import org.elasticsearch.search.sort.SortOrder

import java.util

/**
 * ES client
 */
object EsTest {
  def main(args: Array[String]): Unit = {
    //println(client)
    //put()
    //post()
    //bulk()
    //update()
    //updateByQuery()
    //delete()
    //getById()
    //searchByFilter()
    searchByAggs()
    close()
  }

  /**
   * Batch write
   */
  def bulk(): Unit = {
    val bulkRequest = new BulkRequest()
    val movies = List[Movie](
      Movie("1002", "Harry Potter"),
      Movie("1003", "Spider-man"),
      Movie("1004", "Super-man"),
      Movie("1005", "Captain America")
    )
    for (movie <- movies) {
      val indexRequest = new IndexRequest("movie_test")
      val movieJson = JSON.toJSONString(movie, new SerializeConfig(true))
      indexRequest.source(movieJson, XContentType.JSON)
      // Idempotent write with specified ID, non-idempotent without specifying ID
      indexRequest.id(movie.id)
      bulkRequest.add(indexRequest)
    }
    client.bulk(bulkRequest, RequestOptions.DEFAULT)
  }

  /**
   * update - Single record modification
   */
  def update(): Unit = {
    val updateRequest = new UpdateRequest("movie_test", "1001")
    updateRequest.doc("movie_name", "Fast & Furious 1")
    client.update(updateRequest, RequestOptions.DEFAULT)
  }

  /**
   * update - Conditional modification
   */
  def updateByQuery(): Unit = {
    val updateByQueryRequest = new UpdateByQueryRequest("movie_test")
    //query
    val boolQueryBuilder = QueryBuilders.boolQuery()
    val termQueryBuilder = QueryBuilders.termQuery("movie_name.keyword", "Fast & Furious 1")
    boolQueryBuilder.filter(termQueryBuilder)
    updateByQueryRequest.setQuery(boolQueryBuilder)
    //update
    val params = new util.HashMap[String, AnyRef]()
    params.put("newName", "Fast & Furious 2")
    val script = new Script(
      ScriptType.INLINE,
      Script.DEFAULT_SCRIPT_LANG,
      "ctx._source['movie_name']=params.newName",
      params
    )
    updateByQueryRequest.setScript(script)

    client.updateByQuery(updateByQueryRequest, RequestOptions.DEFAULT)
  }

  /**
   * delete
   */
  def delete(): Unit = {
    val deleteRequest = new DeleteRequest("movie_test", "1001")
    client.delete(deleteRequest, RequestOptions.DEFAULT)
  }

  /**
   * read - Single record search
   */
  def getById(): Unit = {
    val getRequest = new GetRequest("movie_test", "1002")
    val getResponse = client.get(getRequest, RequestOptions.DEFAULT)
    val dataStr = getResponse.getSourceAsString
    println(dataStr)
  }


  /**
   * read - Conditional search
   *
   * Query for "red sea" with imdbScore >= 5.0
   * Highlight the keywords
   * Display the first page with 2 results per page
   * Sort by imdbScore in descending order
   */
  def searchByFilter(): Unit = {
    val searchRequest = new SearchRequest("movie_test")
    val searchSourceBuilder = new SearchSourceBuilder()
    // query
    // bool
    val boolQueryBuilder = QueryBuilders.boolQuery()
    // filter
    val rangeQueryBuilder = QueryBuilders.rangeQuery("imdbScore").gte(5.0)
    boolQueryBuilder.filter(rangeQueryBuilder)
    // must
    val matchQueryBuilder = QueryBuilders.matchQuery("name", "red sea")
    boolQueryBuilder.must(matchQueryBuilder)
    searchSourceBuilder.query(boolQueryBuilder)
    // Paging
    searchSourceBuilder.from(0)
    searchSourceBuilder.size(1)
    // Sort
    searchSourceBuilder.sort("imdbScore", SortOrder.DESC)
    // Highlight
    val highlightBuilder = new HighlightBuilder()
    highlightBuilder.field("name")
    searchSourceBuilder.highlighter(highlightBuilder)

    searchRequest.source(searchSourceBuilder)
    val searchResponse = client.search(searchRequest, RequestOptions.DEFAULT)

    val totalDocs = searchResponse.getHits.getTotalHits.value
    val hits: Array[SearchHit] = searchResponse.getHits.getHits
    for (hit <- hits) {
      val dataJson = hit.getSourceAsString
      val highLightFields = hit.getHighlightFields
      val highlightField = highLightFields.get("name")
      val fragments = highlightField.getFragments
      val highlightValue = fragments(0).toString

      println("Detail data: " + dataJson)
      println("highlight: " + highlightValue)
    }
  }

  /**
   * read - Aggregation search
   *
   * Query the average score of movies each actor has acted in, and sort in descending order
   */
  def searchByAggs(): Unit = {
    val searchRequest = new SearchRequest("movie_test")
    val searchSourceBuilder = new SearchSourceBuilder()
    // Exclude details
    searchSourceBuilder.size(0)
    // group
    val termsAggregationBuilder = AggregationBuilders.terms("groupbyactorname").
      field("actorList.name.keyword").
      size(10).
      order(BucketOrder.aggregation("imdbScore", false))
    // avg
    val avgAggregationBuilder = AggregationBuilders.avg("imdbscoreavg").field("imdbScore")
    termsAggregationBuilder.subAggregation(avgAggregationBuilder)

    searchSourceBuilder.aggregation(termsAggregationBuilder)
    searchRequest.source(searchSourceBuilder)
    val searchResponse = client.search(searchRequest, RequestOptions.DEFAULT)
    val aggregations = searchResponse.getAggregations
    val groupbyactornameParsedTerms = aggregations.get[ParsedTerms]("groupbyactorname")
    val buckets = groupbyactornameParsedTerms.getBuckets
    import scala.collection.JavaConverters._
    for (bucket <- buckets.asScala) {
      val actorName = bucket.getKeyAsString
      val movieCount = bucket.getDocCount
      val aggregations = bucket.getAggregations
      val imdbScoreParsedAvg = aggregations.get[ParsedAvg]("imdbScore")
      val avgScore = imdbScoreParsedAvg.getValue
      println(s"$actorName has acted in a total of $movieCount movies with an average score of $avgScore")
    }
  }

  /**
   * Create - idempotent
   */
  def put(): Unit = {
    val indexRequest = new IndexRequest()
    // Specify the index
    indexRequest.index("movie_test")
    // Specify the doc
    val movie = Movie("1001", "Fast & Furious")
    val movieJson = JSON.toJSONString(movie, new SerializeConfig(true))
    indexRequest.source(movieJson, XContentType.JSON)
    // Specify the docid
    indexRequest.id("1001")
    client.index(indexRequest, RequestOptions.DEFAULT)
  }

  /**
   * Create - non-idempotent
   */
  def post(): Unit = {
    val indexRequest = new IndexRequest()
    // Specify the index
    indexRequest.index("movie_test")
    // Specify the doc
    val movie = Movie("1001", "Fast & Furious")
    val movieJson = JSON.toJSONString(movie, new SerializeConfig(true))
    indexRequest.source(movieJson, XContentType.JSON)
    client.index(indexRequest, RequestOptions.DEFAULT)
  }

  // Client object
  var client: RestHighLevelClient = creat()

  // creat client object
  def creat(): RestHighLevelClient = {
    val restClientBuilder = RestClient.builder(new HttpHost("hadoop102", 9200))
    val client = new RestHighLevelClient(restClientBuilder)
    client
  }

  // close client object
  def close(): Unit = {
    if(client != null) client.close()
  }
}

case class Movie(id: String, movie_name: String)
