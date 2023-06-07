package com.muzhe.gmall.realtime.util

import java.util.ResourceBundle

/**
 * @Author muyiacc
 * @Date 2023/6/6 006 18:19
 * @Description: TODO 配置文件解析类
 */

/**
 * 配置文件解析类
 */
object MyPropsUtils {

  private val bundle: ResourceBundle = ResourceBundle.getBundle("config")

  def apply(propsKey:String): String = {
    bundle.getString(propsKey)
  }



}
