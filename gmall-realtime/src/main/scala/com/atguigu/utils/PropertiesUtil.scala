package com.atguigu.utils

import java.io.InputStreamReader
import java.util.Properties


object PropertiesUtil {
  def load(propertyName: String) ={
    val prop: Properties = new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertyName),"UTF-8"))
    prop
  }


}
