package cn.whaley.bi.logsys.metadata.repository;


import cn.whaley.bi.logsys.metadata.entity.LogFileKeyFieldValueEntity;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.*;

/**
 * Created by fj on 2017/6/14.
 */
@Repository
public class LogFileKeyFieldValueRepo extends MetadataBaseRepo<LogFileKeyFieldValueEntity> {


    public LogFileKeyFieldValueRepo() {
        super(LogFileKeyFieldValueEntity.class);
    }


    public List<LogFileKeyFieldValueEntity> findByTaskId(String taskId) {
        Map<String, Object> where = new HashMap<>();
        where.put("taskId", taskId);
        where.put("isDeleted", false);
        List<LogFileKeyFieldValueEntity> entities = select(where);
        return entities;
    }

    /**
     * 删除操作,taskId和logPath两者不能同时为空
     * @param taskId
     * @param logPath
     * @return
     */
    public Integer delete(String taskId, String logPath) {
        Map<String, Object> where = new HashMap<>();
        if (StringUtils.hasText(taskId)) {
            where.put("taskId", taskId);
        }
        if (StringUtils.hasText(logPath)) {
            where.put("logPath", logPath);
        }

        Assert.isTrue(where.size() > 0, "taskId logPath 参数不能同时为空.");

        Map<String, Object> update = new HashMap<>();
        where.put("isDeleted", true);
        where.put("updateTime", new Date());

        Integer ret = update(LogFileKeyFieldValueEntity.KEY_FIELDS, update, where);
        return ret;
    }

}
