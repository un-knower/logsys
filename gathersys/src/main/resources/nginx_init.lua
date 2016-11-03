--
-- Created by IntelliJ IDEA.
-- User: fj
-- Date: 16/10/31
-- Time: 上午11:59
-- To change this template use File | Settings | File Templates.
--



luabit = require "bit"

sharedDict = ngx.shared.sharedDict;
sharedDict:set("msg_site", "");

function split(s, delim)
    if type(delim) ~= "string" or string.len(delim) <= 0 then
        return
    end

    local start = 1
    local t = {}
    while true do
        local pos = string.find(s, delim, start, true) -- plain find
        if not pos then
            break
        end

        table.insert(t, string.sub(s, start, pos - 1))
        start = pos + string.len(delim)
    end
    table.insert(t, string.sub(s, start))

    return t
end

function paddingLeft(str, len, chr)
    local restLen = len - string.len(str);
    if restLen > 0 then
        str = string.rep(chr, restLen) .. str;
    end;
    return str;
end

;

function createMsgId()
    local ipStr = split(ngx.var.server_addr, ".")

    local hexIP1 = paddingLeft(string.format("%X", ipStr[1]), 2, "0")
    local hexIP2 = paddingLeft(string.format("%X", ipStr[2]), 2, "0")
    local hexIP3 = paddingLeft(string.format("%X", ipStr[3]), 2, "0")
    local hexIP4 = paddingLeft(string.format("%X", ipStr[4]), 2, "0")

    local v0 = ngx.now() * 1000;
    local v1 = hexIP1 .. hexIP2 .. hexIP3 .. hexIP4
    local v2 = ngx.var.pid % 65536;
    local v3 = ngx.var.connection_requests % 65536
    local v4 = ngx.var.connection % 65536

    local hexV0 = paddingLeft(string.format("%X", v0), 12, "0")
    local hexV2 = paddingLeft(string.format("%X", v2), 4, "0")
    local hexV3 = paddingLeft(string.format("%X", v3), 4, "0")
    local hexV4 = paddingLeft(string.format("%X", v4), 4, "0")

    local msgId = hexV0 .. v1 .. hexV2 .. hexV3 .. hexV4

    return msgId
end

function buildMsgInfo()

    local msgRemoteIp = ngx.var.remote_addr
    if msgRemoteIp == "" then
        msgRemoteIp = ngx.var.http_x_forwarded_for
    end

    ngx.var.msg_id = createMsgId(ngx)
    ngx.var.msg_site = ngx.var.server_addr
    ngx.var.msg_remote_ip = msgRemoteIp
    ngx.var.msg_receive_time = ngx.now() * 1000
end

function checkMsgMd5(requiredMd5)
    local headers = ngx.req.get_headers()
    local md5 = headers["md5"]
    if (md5 == nil) then
        return requiredMd5 == false
    end
    local ts = headers["ts"]
    local req_body = ngx.req.get_body_data()
    local md5Calc = ngx.md5(ts .. req_body)
    return md5Calc == md5
end

function loadDbInfo()
    local mysql = require "resty.mysql"
    local db, err = mysql:new()
    if not db then
        ngx.log(ngx.ERR,"failed to instantiate mysql: ", err)
        return
    end
    local ok, err, errno, sqlstate = db:connect{
        host = "127.0.0.1",
        port = 3306,
        database = "medusa",
        user = "root",
        password = "316239fJ",
        max_packet_size = 1024 * 1024,
        characterEncoding="utf8",
        useUnicode=true
    }
    if not ok then
        ngx.log(ngx.ERR,"failed to connect: ", err, ": ", errno, " ", sqlstate)
        return
    end
    sql = "select * from t_machines where id = 2"

    res, err, errno, sqlstate = db:query(sql)
    if not res then
        ngx.log(ngx.ERR,"bad result: ", err, ": ", errno, ": ", sqlstate, ".")
        return
    end

    local cjson = require "cjson"
    ngx.header.content_type="application/json;charset=utf8"
    ngx.say(cjson.encode(res))

    local ok, err = db:set_keepalive(0, 100)
    if not ok then
        ngx.log(ngx.ERR,"failed to set keepalive: ", err)
        return
    end

end

