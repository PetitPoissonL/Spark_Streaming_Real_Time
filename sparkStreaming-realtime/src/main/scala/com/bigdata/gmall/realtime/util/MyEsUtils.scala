package com.bigdata.gmall.realtime.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder

import java.util
import scala.collection.Searching.SearchResult
import scala.collection.mutable.ListBuffer

/**
 * Elasticsearch utility class for read and write operations on Elasticsearch
 */
object MyEsUtils {
  // object of client
  val esClient: RestHighLevelClient = build()

  // creat ES client object
  def build(): RestHighLevelClient = {
    val host: String = MyPropsUtils(MyConfig.ES_HOST)
    val port: String = MyPropsUtils(MyConfig.ES_PORT)
    val restClientBuilder: RestClientBuilder = RestClient.builder(new HttpHost(host, port.toInt))
    val client = new RestHighLevelClient(restClientBuilder)
    client
  }

  // close ES client
  def close(): Unit = {
    if(esClient != null) esClient.close()
  }
  def bulkSave(indexName: String, docs: List[(String, AnyRef)]): Unit = {
    val bulkRequest = new BulkRequest(indexName)
    for ((docId, docObj) <- docs) {
      val indexRequest = new IndexRequest()
      val dataJson: String = JSON.toJSONString(docObj, new SerializeConfig(true))
      indexRequest.source(dataJson, XContentType.JSON)
      indexRequest.id(docId)
      bulkRequest.add(indexRequest)
    }
    esClient.bulk(bulkRequest, RequestOptions.DEFAULT)
  }

  /**
   * Query specific fields
   */
  def searchField(indexName: String, fieldName: String): List[String] = {
    // Check if the index exists
    val getIndexRequest = new GetIndexRequest(indexName)
    val isExists: Boolean = esClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT)
    if(!isExists){
      return null
    }

    val fields: ListBuffer[String] = ListBuffer[String]()
    val searchRequest = new SearchRequest(indexName)
    val searchSourceBuilder = new SearchSourceBuilder()
    searchSourceBuilder.fetchSource(fieldName, null).size(100000)
    searchRequest.source(searchSourceBuilder)
    val searchResponse: SearchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT)
    val hits: Array[SearchHit] = searchResponse.getHits.getHits
    for (hit <- hits) {
      val sourceMap: util.Map[String, AnyRef] = hit.getSourceAsMap
      val field: String = sourceMap.get(fieldName).toString
      fields.append(field)
    }
    fields.toList
  }

  def main(args: Array[String]): Unit = {
    searchField("gmall_dau_info_1018_2022-03-28", "mid")
  }

}
