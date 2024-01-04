package com.bigdata.gmall.realtime.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType

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

}
