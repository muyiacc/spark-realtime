package com.muzhe.gmall.realtime.bean

/**
 * @Author muyiacc
 * @Date 2023/6/7 007 10:18
 * @Description: TODO 封装页面数据
 */
case class PageDisplayLog(
                  mid : String,
                  user_id : String,
                  province_id : String,
                  channel : String,
                  is_new : String,
                  model : String,
                  operate_system : String,
                  version_code : String,
                  brand : String, // 平台 pdf没有
                  page_id : String,
                  last_page_id : String,
                  page_item : String,
                  page_item_type : String,
                  source_type : String,  // pdf 没有
                  during_time : Long,
                  display_type: String,
                  display_item: String,
                  display_item_type: String,
                  display_order: Long, // pdf 为String,但是直接获取的是数字类型,所以我改成Long
                  display_pos_id: Long, // pdf 为String,但是直接获取的是数字类型,所以我改成Long
                  ts : Long
                  )
