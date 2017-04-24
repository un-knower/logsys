--
-- Created by IntelliJ IDEA.
-- User: fj
-- Date: 16/10/31
-- Time: 上午11:59
-- To change this template use File | Settings | File Templates.
--

bit = require("bit")
ffi = require("ffi")

sharedDict = ngx.shared.sharedDict;

--1.0版本md5签名key
sharedDict:set("log-sign-key-md5-1.0", "92DOV+sOk160j=430+DM!ZzESf@XkEsn#cKanpB$KFB6%D8z4C^xg7cs6&7wn0A4A*iR9M3j)pLs]ll5E9aFlU(dE0[QKxHZzC.CaO/2Ym3|Tk<YyGZR>WuRUmI?x2s:Cg;YEA-hZubmGnWgXE");

base64Chars = {
    "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L",
    "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l",
    "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "+", "/"
}

--执行子请求
function doSubrequest(mainLocaition, subLocation)
    ngx.req.read_body();
    if ngx.var.request_method == "POST" then
        params = { method = ngx.HTTP_POST, copy_all_vars = true }
    else
        params = { method = ngx.HTTP_GET, copy_all_vars = true }
    end

    --ngx.req.set_header("Content-Type", ngx.var.content_type);
    --ngx.req.set_header("Accept", "application/json");

    local res0, res1 = ngx.location.capture_multi({
        { mainLocaition, params },
        { subLocation, params },
    })

    if res1.status ~= ngx.HTTP_OK then
        ngx.log(ngx.ERR, subLocation .. " err[" .. res1.status .. "]: "
        .. ngx.var.request_method .. " "
        .. getOrElse(ngx.var.content_type,"UNKNOWN") .. " "
        .. res1.body)
    end

    ngx.status = res0.status
    ngx.say(res0.body)
end

--构建消息相关的上下文信息
function buildMsgInfo()

    local msgRemoteIp = ngx.var.http_x_forwarded_for
    if msgRemoteIp == "" then
        msgRemoteIp = ngx.var.remote_addr
    end

    ngx.req.read_body();

    ngx.var.msg_id = createMsgId()
    ngx.var.msg_site = ngx.var.server_addr
    ngx.var.msg_remote_ip = msgRemoteIp
    ngx.var.msg_receive_time = ngx.now() * 1000
    ngx.var.msg_sign_flag = doMsgSign()
    ngx.var.msg_req_body = getOrElse(ngx.req.get_body_data(),"{}")

end

--创建消息ID，16字节的base64编码，总长24位（8字节时间戳+4字节IP地址+2字节进程号+2字节TCP连接序列号+2字节TCP连接请求计数器）
function createMsgId()
    local u64 = ffi.new("uint64_t[11]");

    local ipStr = split(ngx.var.server_addr, ".")
    local pid = ngx.worker.pid() % 65536;
    local connection = ngx.var.connection % 65536
    local requests = ngx.var.connection_requests % 65536

    u64[0] = ngx.now() * 1000;
    u64[1] = tonumber(ipStr[1]);
    u64[2] = tonumber(ipStr[2]);
    u64[3] = tonumber(ipStr[3]);
    u64[4] = tonumber(ipStr[4]);
    u64[5] = tonumber(pid);
    u64[6] = tonumber(connection);
    u64[7] = tonumber(requests);

    u64[8] = u64[0]
    u64[9] = bit.bor(bit.lshift(u64[1], 56), bit.lshift(u64[2], 48), bit.lshift(u64[3], 40), bit.lshift(u64[4], 32), bit.lshift(u64[5], 16), u64[6])
    u64[10] = bit.lshift(u64[7], 48)
    local msgId = getMsgBase64Char(u64[8], u64[9], u64[10])
    return msgId
    --local str = bit.tohex(u64[8]) .. "|" .. bit.tohex(u64[9]) .. "|" .. bit.tohex(u64[10]) .. " -> " .. msgId
    --return str
end


--消息体签名验证
--@return -1:签名校验失败； 0：没有做签名校验； 1：签名校验成功
function doMsgSign()
    local headers = ngx.req.get_headers()
    local logSignMethod = headers["log-sign-method"]
    local logSignVersion = headers["log-sign-version"]
    local logSignTs = headers["log-sign-ts"]
    local logSignValue = headers["log-sign-value"]


    --头中不包含签名信息,不做签名校验
    if (logSignMethod == nil) then
        return 0
    end

    --md5签名
    if (logSignMethod == "md5") then
        if (logSignVersion == nil or logSignTs == nil or logSignValue == nil) then
            return -1
        end

        local logSignKey = sharedDict:get("log-sign-key-" .. logSignMethod .. "-" .. logSignVersion)
        if (logSignKey == nil) then
            return -1
        end

        ngx.req.read_body();
        local req_body = ngx.req.get_body_data()
        if (req_body == nil) then
            return -1
        end

        local md5 = ngx.md5(logSignKey .. logSignTs .. req_body)
        if (md5 == logSignValue) then
            return 1
        else
            return -1
        end
    end

    --目前只实现了md5签名算法
    return -1
end

--字符串分隔
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

--左侧填充
function paddingLeft(str, len, chr)
    local restLen = len - string.len(str);
    if restLen > 0 then
        str = string.rep(chr, restLen) .. str;
    end;
    return string.sub(str, str.length - len + 1);
end

--如果value为nil，则返回orValue，否则返回value
function getOrElse(value, orValue)
    if (value == nil) then
        return orValue
    else
        return value
    end
end

--获取64位整形数指定bit范围内的值
function getBitRangeValue(uint64Value, fromIndex, toIndex)
    local mask = bit.lshift(1, toIndex - fromIndex + 1) - 1
    return tonumber(bit.band(bit.rshift(uint64Value, 64 - toIndex), mask))
end


--获取64位整形数6位bit范围内的十六进制值
function get6BitRangeValue(uint64Value, rangeIndex)
    local fromIndex = (rangeIndex - 1) * 6 + 1
    local toIndex = fromIndex + 5
    local v = getBitRangeValue(uint64Value, fromIndex, toIndex)
    return bit.tohex(v)
end

--获取64位整形数6位bit范围内的十六进制值
function get6BitRangeChar(uint64Value, rangeFrom, rangeIndex)
    local fromIndex = rangeFrom + (rangeIndex - 1) * 6
    local toIndex = fromIndex + 5
    local v = getBitRangeValue(uint64Value, fromIndex, toIndex)
    return base64Chars[tonumber(v + 1)]
end


--获取64位整形数6位bit范围内的十六进制值
function getMsgBase64Char(uint64_1, uint64_2, uint64_3)
    local str = get6BitRangeChar(uint64_1, 1, 1) .. get6BitRangeChar(uint64_1, 1, 2)
            .. get6BitRangeChar(uint64_1, 1, 3) .. get6BitRangeChar(uint64_1, 1, 4)
            .. get6BitRangeChar(uint64_1, 1, 5) .. get6BitRangeChar(uint64_1, 1, 6)
            .. get6BitRangeChar(uint64_1, 1, 7) .. get6BitRangeChar(uint64_1, 1, 8)
            .. get6BitRangeChar(uint64_1, 1, 9) .. get6BitRangeChar(uint64_1, 1, 10)
            .. base64Chars[bit.bor(bit.lshift(getBitRangeValue(uint64_1, 61, 64), 2), getBitRangeValue(uint64_2, 1, 2)) + 1]
            .. get6BitRangeChar(uint64_2, 3, 1)
            .. get6BitRangeChar(uint64_2, 3, 2)
            .. get6BitRangeChar(uint64_2, 3, 3)
            .. get6BitRangeChar(uint64_2, 3, 4)
            .. get6BitRangeChar(uint64_2, 3, 5)
            .. get6BitRangeChar(uint64_2, 3, 6)
            .. get6BitRangeChar(uint64_2, 3, 7)
            .. get6BitRangeChar(uint64_2, 3, 8)
            .. get6BitRangeChar(uint64_2, 3, 9)
            .. get6BitRangeChar(uint64_2, 3, 10)
            .. base64Chars[bit.bor(bit.lshift(getBitRangeValue(uint64_2, 63, 64), 4), getBitRangeValue(uint64_3, 1, 4)) + 1]
            .. get6BitRangeChar(uint64_3, 5, 1)
            .. get6BitRangeChar(uint64_3, 5, 2)

    return str
end


function test()
    ngx.req.read_body();
    local reqBody = ngx.var.request_body;
    local decoded = cjson.decode(reqBody)
    return cjson.decode(decoded)
end
