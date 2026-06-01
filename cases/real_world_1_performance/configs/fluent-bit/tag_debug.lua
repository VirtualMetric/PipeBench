-- real_world_1 tagger — fluent-bit's regex parser captures "pri" (priority
-- as string), not severity name. severity = pri & 7, debug = severity 7.
-- Matches the conditional-tagging step in the other subjects' pipelines.
function tag_debug(tag, ts, record)
    local pri = tonumber(record["pri"])
    if pri and (pri % 8 == 7) then
        record["dropped"] = true
    end
    return 2, ts, record
end
