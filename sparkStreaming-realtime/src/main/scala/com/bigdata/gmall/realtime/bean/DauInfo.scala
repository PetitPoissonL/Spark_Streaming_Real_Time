package com.bigdata.gmall.realtime.bean

case class DauInfo(
                    //Basic page visit log data
                    var mid: String,
                    var user_id: String,
                    var province_id: String,
                    var channel: String,
                    var is_new: String,
                    var model: String,
                    var operate_system: String,
                    var version_code: String,
                    var page_id: String,
                    var page_item: String,
                    var page_item_type: String,
                    var brand: String,
                    var sourceType: String,
                    var during_time: Long,
                    // User gender and age
                    var user_gender: String,
                    var user_age: String,
                    // Location information
                    var province_name: String,
                    var province_iso_code: String,
                    var province_3166_2: String,
                    var province_area_code: String,
                    // Date
                    var dt: String,
                    var hr: String,
                    var ts: Long
                  ) {
  def this() {
    this(null, null, null, null, null, null, null, null, null, null, null, null, null, 0L, null, null, null, null, null, null, null, null, 0L)
  }

}
