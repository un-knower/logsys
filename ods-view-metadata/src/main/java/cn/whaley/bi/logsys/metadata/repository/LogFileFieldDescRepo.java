package cn.whaley.bi.logsys.metadata.repository;


import cn.whaley.bi.logsys.metadata.entity.LogFileFieldDescEntity;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by fj on 2017/6/14.
 */
@Repository
public class LogFileFieldDescRepo extends MetadataBaseRepo<LogFileFieldDescEntity> {
    public LogFileFieldDescRepo() {
        super(LogFileFieldDescEntity.class);
    }

    public List<LogFileFieldDescEntity> findByTaskId(String taskId) {
        Map<String, Object> where = new HashMap<>();
        where.put("taskId", taskId);
        where.put("isDeleted", false);
        List<LogFileFieldDescEntity> entities = select(where);
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
        String sql = "delete from " + LogFileFieldDescEntity.TABLE_NAME + " where taskId=?";
        Integer ret = update(sql, taskId);
        return ret;
    }

}
