package cn.whaley.bi.logsys.metadata.entity;

import com.alibaba.fastjson.JSON;

/**
 * Created by fj on 2017/7/3.
 */
public class ResponseEntity<T> {
    private Integer code = 0;
    private String message = "";
    private T result;

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public T getResult() {
        return result;
    }

    public void setResult(T result) {
        this.result = result;
    }
}
