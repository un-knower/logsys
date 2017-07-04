package cn.whaley.bi.logsys.metadata.repository;


import cn.whaley.bi.logsys.metadata.entity.LogFileFieldDescEntity;
import cn.whaley.bi.logsys.metadata.entity.LogFileTaskInfoEntity;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.*;

/**
 * Created by fj on 2017/6/14.
 */
@Repository
public class LogFileTaskInfoRepo extends MetadataBaseRepo<LogFileTaskInfoEntity> {


    public LogFileTaskInfoRepo() {
        super(LogFileTaskInfoEntity.class);
    }

    public List<LogFileTaskInfoEntity> findByTaskId(String taskId) {
        Map<String, Object> where = new HashMap<>();
        where.put("taskId", taskId);
        where.put("isDeleted", false);
        List<LogFileTaskInfoEntity> entities = select(where);
        return entities;
    }

    /**
     * 删除操作
     * @param taskId
     * @return
     */
    public Integer delete(String taskId) {
        Map<String, Object> where = new HashMap<>();
        if (StringUtils.hasText(taskId)) {
            where.put("taskId", taskId);
        }

        Assert.isTrue(where.size() > 0, "taskId must be not null.");

        Map<String, Object> update = new HashMap<>();
        where.put("isDeleted", true);
        where.put("updateTime", new Date());

        Integer ret = update(LogFileTaskInfoEntity.KEY_FIELDS, update, where);
        return ret;
    }


}
