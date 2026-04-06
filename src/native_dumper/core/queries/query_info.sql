select
    hostname as host
  , query_kind as kind
  , query_duration_ms / 1000 as duration
  , memory_usage as memory
  , result_bytes as storage
  , result_rows as rows
from system.query_log
where
    http_user_agent = '{user_agent}'
    and type = 'QueryFinish'
    and query_id = '{query_id}'