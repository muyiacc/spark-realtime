package com.muzhe.gmall.realtime.bean

/**
 * @Author muyiacc
 * @Date 2023/6/7 007 11:10
 * @Description: TODO 封装页面数据
 */
case class PageLog(
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
                  ts : Long
                  )
