package com.atguigu.utils

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index}


object MyEsUtil {
  private val serverUri = "http://hadoop102:9200"
//  private var jestClient: JestClient = null
  private var factory:JestClientFactory = null

  /**
   * 获取es连接对象
   */
  def getJestClient(): JestClient = {
    var jestClient: JestClient = null
    if (factory == null) {
      factory = new JestClientFactory()
      val build: HttpClientConfig = new HttpClientConfig.Builder(serverUri).build
      factory.setHttpClientConfig(build)
    }
    jestClient = factory.getObject
    jestClient

  }

  /**
   * 关闭es连接对象
   */
  def closeJestClient(jestClient : JestClient) = {
    if (jestClient != null)
    jestClient.shutdownClient()
  }

  def insertBulk(indexName: String, list: List[(String, Any)]): Unit = {
    if (list.size <= 0) {
      return
    }

    // 获取jest连接
    val jestClient: JestClient = getJestClient()


    val builder: Bulk.Builder = new Bulk.Builder()
      .defaultIndex(indexName)
      .defaultType("_doc")
    for ((id, doc) <- list) {
      val index: Index = new Index.Builder(doc).id(id).build()
      builder.addAction(index)
    }
    val bulk: Bulk = builder.build()
    try {
      jestClient.execute(bulk)
    } finally {
      closeJestClient(jestClient)
    }

  }

}