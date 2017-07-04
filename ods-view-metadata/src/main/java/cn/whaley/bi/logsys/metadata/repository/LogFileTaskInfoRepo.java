package cn.whaley.bi.logsys.metadata.repository;


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
     *
     * @param taskId
     * @return
     */
    public Integer delete(String taskId) {
        Assert.isTrue(StringUtils.hasText(taskId), "taskId must be provided.");
        String sql = "delete from " + LogFileTaskInfoEntity.TABLE_NAME + " where taskId=?";
        Integer ret = update(sql, taskId);
        return ret;
    }


}
