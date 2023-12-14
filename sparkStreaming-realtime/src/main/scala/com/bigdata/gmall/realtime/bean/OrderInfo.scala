package com.bigdata.gmall.realtime.bean

case class OrderInfo(
                      id: Long = 0L,
                      province_id: Long = 0L,
                      order_status: String = null,
                      user_id: Long = 0L,
                      total_amount: Double = 0D,
                      activity_reduce_amount: Double = 0D,
                      coupon_reduce_amount: Double = 0D,
                      original_total_amount: Double = 0D,
                      feight_fee: Double = 0D,
                      feight_fee_reduce: Double = 0D,
                      expire_time: String = null,
                      refundable_time: String = null,
                      create_time: String = null,
                      operate_time: String = null,

                      var create_date: String = null, // Processing other fields to obtain
                      var create_hour: String = null,
                      var province_name: String = null, // Retrieve from the dimension table
                      var province_area_code: String = null,
                      var province_3166_2_code: String = null,
                      var province_iso_code: String = null,
                      var user_age: Int = 0, // Retrieve from the dimension table
                      var user_gender: String = null
) {

}
