# Запрос собирает данные по сессиям в разрезе utm-меток
SELECT 
  IFNULL(REGEXP_EXTRACT(  
        preprocessing.DECODE_URI_COMPONENT((SELECT MAX(IF(key="page_location",value.string_value , NULL)) FROM UNNEST(a.event_params))), 
        r'[&\?]utm_source=([^&]+)'),
        IFNULL((SELECT MAX(IF(key="source",value.string_value , NULL)) FROM UNNEST(b.event_params)),"(direct)")
        ) as source,
  IFNULL(REGEXP_EXTRACT(  
        preprocessing.DECODE_URI_COMPONENT((SELECT MAX(IF(key="page_location",value.string_value , NULL)) FROM UNNEST(a.event_params))), 
        r'[&\?]utm_medium=([^&]+)'),
        IFNULL((SELECT MAX(IF(key="medium",value.string_value , NULL)) FROM UNNEST(b.event_params)),"(none)")
        )
        as medium,
  IFNULL(REGEXP_EXTRACT(  
        preprocessing.DECODE_URI_COMPONENT((SELECT MAX(IF(key="page_location",value.string_value , NULL)) FROM UNNEST(a.event_params))), 
        r'[&\?]utm_campaign=([^&]+)'),
        IFNULL((SELECT MAX(IF(key="campaign",value.string_value , NULL)) FROM UNNEST(b.event_params)),"(direct)")
        ) as campaign,
  IFNULL(REGEXP_EXTRACT(  
        preprocessing.DECODE_URI_COMPONENT((SELECT MAX(IF(key="page_location",value.string_value , NULL)) FROM UNNEST(a.event_params))), 
        r'[&\?]utm_term=([^&]+)'),
        (SELECT MAX(IF(key="term",value.string_value , NULL)) FROM UNNEST(b.event_params))
        ) as term,
  IF(REGEXP_EXTRACT(  
        preprocessing.DECODE_URI_COMPONENT((SELECT MAX(IF(key="page_location",value.string_value , NULL)) FROM UNNEST(a.event_params))), 
        r'[&\?]utm_content=([^&]+)')="voronka_love2","voronka_love",
        REGEXP_EXTRACT(  
        preprocessing.DECODE_URI_COMPONENT((SELECT MAX(IF(key="page_location",value.string_value , NULL)) FROM UNNEST(a.event_params))), 
        r'[&\?]utm_content=([^&]+)')
        ) as content,  
  REGEXP_EXTRACT(  
        preprocessing.DECODE_URI_COMPONENT((SELECT MAX(IF(key="page_location",value.string_value , NULL)) FROM UNNEST(a.event_params))), 
        r'[&\?]utm_placement=([^&]+)') as placement,  
  REGEXP_EXTRACT(  
        preprocessing.DECODE_URI_COMPONENT((SELECT MAX(IF(key="page_location",value.string_value , NULL)) FROM UNNEST(a.event_params))), 
        r'[&\?]vkid=([^&]+)') as vkid,  
        PARSE_DATE ('%Y%m%d', a.event_date) as date,
  a.user_pseudo_id,
  a.event_timestamp,
  (SELECT MAX(IF(key="ga_session_id",value.int_value , NULL)) FROM UNNEST(a.event_params)) AS session_id,
  REGEXP_REPLACE(preprocessing.DECODE_URI_COMPONENT((SELECT MAX(IF(key="page_location",value.string_value , NULL)) FROM UNNEST(a.event_params))),"\\?.*","") as url,
  preprocessing.DECODE_URI_COMPONENT((SELECT MAX(IF(key="page_referrer",value.string_value , NULL)) FROM UNNEST(a.event_params))) as page_referrer,
FROM `analytics_303258175.events_*` a
  left join `analytics_303258175.events_*` b using (_TABLE_SUFFIX,event_timestamp,user_pseudo_id)
  WHERE
     _TABLE_SUFFIX <= FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)) 
  and   a.event_name in ("session_start") and b.event_name in ("page_view")
