package cn.whaley.bi.logsys.metadata.repository;

import cn.whaley.bi.logsys.metadata.entity.LogTabFieldDescEntity;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

/**
 * Created by fj on 2017/6/14.
 */
@Repository
public class LogTabFieldDescRepo extends MetadataBaseRepo<LogTabFieldDescEntity> {
    public LogTabFieldDescRepo() {
        super(LogTabFieldDescEntity.class);
    }

    /**
     * 查找某批次任务产生的字段定义
     *
     * @param taskId
     * @return
     */
    @Transactional(readOnly = true)
    public List<LogTabFieldDescEntity> findByTaskId(String taskId) {
        Map<String, Object> where = new HashMap<>();
        where.put("taskId", taskId);
        List<LogTabFieldDescEntity> entities = select(where);
        return entities;
    }


    @Transactional(readOnly = false)
    public Integer deleteByTaskId(String taskId) {
        Set<String> keys = new HashSet<>();
        keys.addAll(Arrays.asList("dbName,tabName,fieldName,seq".split(",")));

        Map<String, Object> updates = new HashMap<>();
        updates.put("isDeleted", true);

        Map<String, Object> wheres = new HashMap<>();
        wheres.put("taskId", taskId);
        wheres.put("isDeleted", false);

        return update(keys, updates, wheres);

    }

}
