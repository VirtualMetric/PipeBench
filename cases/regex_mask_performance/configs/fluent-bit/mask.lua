-- Regex-mask filter — equivalent of gsub "CONN=[0-9]+" -> "CONN=***" on the raw log.
function mask_conn(tag, timestamp, record)
    if record["log"] then
        record["log"] = string.gsub(record["log"], "CONN=%d+", "CONN=***")
    end
    return 2, timestamp, record
end
