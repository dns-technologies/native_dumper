select 
    toBool((toUInt32(splitByChar('.', server_version)[1]) * 10000) + 
    (toUInt32(splitByChar('.', server_version)[2]) * 100) + 
    toUInt32(splitByChar('.', server_version)[3]) >= 251100) as headers_memory
  , version() as server_version