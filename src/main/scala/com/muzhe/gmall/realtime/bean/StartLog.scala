package com.muzhe.gmall.realtime.bean

/**
 * @Author muyiacc
 * @Date 2023/6/7 007 11:49
 * @Description: TODO
 */
case class StartLog(
                    user_id:String,
                    province_id:String,
                    channel:String,
                    is_new:String,
                    model:String,
                    operate_system:String,
                    version_code:String,
                    entry:String,
                    open_ad_id: Long, // pdf 是String, 但是实际上是Long,所以我改为Long
                    loading_time_ms:Long,
                    open_ad_ms:Long,
                    open_ad_skip_ms:Long,
                    ts:Long
                   )
