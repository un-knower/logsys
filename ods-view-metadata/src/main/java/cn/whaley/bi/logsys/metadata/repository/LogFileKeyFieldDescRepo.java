package cn.whaley.bi.logsys.metadata.repository;


import cn.whaley.bi.logsys.metadata.entity.LogFileKeyFieldDescEntity;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by fj on 2017/6/14.
 */
@Repository
public class LogFileKeyFieldDescRepo extends MetadataBaseRepo<LogFileKeyFieldDescEntity> {
    public LogFileKeyFieldDescRepo() {
        super(LogFileKeyFieldDescEntity.class);
    }


    public List<LogFileKeyFieldDescEntity> findByTaskId(String taskId) {
        Map<String, Object> where = new HashMap<>();
        where.put("taskId", taskId);
        where.put("isDeleted", false);
        List<LogFileKeyFieldDescEntity> entities = select(where);
        return entities;
    }
}
