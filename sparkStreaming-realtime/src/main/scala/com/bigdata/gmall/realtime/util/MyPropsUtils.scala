package com.bigdata.gmall.realtime.util

import java.util.ResourceBundle

/**
 * Configuration File Parser Class
 */
object MyPropsUtils {
  private val bundle: ResourceBundle = ResourceBundle.getBundle("config")

  def apply(propsKey: String) : String = {
    bundle.getString(propsKey)
  }

  def main(args: Array[String]): Unit = {
    println(MyPropsUtils("kafka.bootstrap-service"))
  }
}
