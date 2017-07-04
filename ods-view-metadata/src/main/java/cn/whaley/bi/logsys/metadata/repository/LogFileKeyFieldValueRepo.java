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
     * 删除操作
     *
     * @param taskId
     * @return
     */
    public Integer delete(String taskId) {

        Assert.isTrue(StringUtils.hasText(taskId), "taskId must be provided.");

        Map<String, Object> update = new HashMap<>();
        update.put("isDeleted", true);
        update.put("updateTime", new Date());

        Map<String, Object> where = new HashMap<>();
        where.put("taskId", taskId);

        Integer ret = update(LogFileKeyFieldValueEntity.KEY_FIELDS, update, where);
        return ret;
    }

}
