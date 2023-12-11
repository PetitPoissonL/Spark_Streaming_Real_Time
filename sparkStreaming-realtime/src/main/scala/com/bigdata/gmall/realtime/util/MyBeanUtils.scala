package com.bigdata.gmall.realtime.util

import com.bigdata.gmall.realtime.bean.{DauInfo, PageLog}

import java.lang.reflect.{Field, Method, Modifier}
import scala.util.control.Breaks

/**
 * Implementing object property copying
 */
object MyBeanUtils {
  /**
   * test
   */
  def main(args: Array[String]): Unit = {
    val pageLog: PageLog = PageLog("mid1001", "uid101", "prov101", null, null, null, null, null, null, null, null, null, null, 0L, null, 123456)

    val dauInfo = new DauInfo()
    println("before copy: "+dauInfo)

    copyProperties(pageLog, dauInfo)

    println("after copy: "+dauInfo)
  }

  /**
   * Copy the values of properties from 'srcObj' to the corresponding properties in 'destObj'
   */
  def copyProperties(srcObj : AnyRef, destObj : AnyRef): Unit = {
    if(srcObj == null || destObj == null){
      return
    }
    // Retrieve all properties from 'srcObj'
    val srcFields: Array[Field] = srcObj.getClass.getDeclaredFields

    // Handle the copying of each property
    for (srcField <- srcFields) {
      Breaks.breakable{
        // Scala automatically provides get and set methods for properties within a class
        // get: fieldname()
        // set: fieldname_$eq(Parameter_types)
        var getMethodName: String = srcField.getName
        var setMethodName: String = srcField.getName + "_$eq"

        // Retrieve the get method object from 'srcObj'
        val getMethod: Method = srcObj.getClass.getDeclaredMethod(getMethodName)
        // Retrieve the set method object from 'destObj'
        val setMethod: Method =
          try {
            destObj.getClass.getDeclaredMethod(setMethodName, srcField.getType)
          } catch { // NoSuchMethodException
            // If unable to retrieve the field, skip it and proceed to the next field
            case ex: Exception => Breaks.break()
          }
        // Ignore 'val' properties
        val destField: Field = destObj.getClass.getDeclaredField(srcField.getName)
        if(destField.getModifiers.equals(Modifier.FINAL)){
          Breaks.break()
        }

        // Call the get method to retrieve the value of the property from 'srcObj,' and then call the set method to assign the retrieved value to the property of 'destObj'
        setMethod.invoke(destObj, getMethod.invoke(srcObj))
      }
    }
  }
}
