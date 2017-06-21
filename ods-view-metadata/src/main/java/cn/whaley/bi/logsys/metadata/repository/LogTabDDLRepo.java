package cn.whaley.bi.logsys.metadata.repository;

import cn.whaley.bi.logsys.metadata.entity.LogTabDDLEntity;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

/**
 * Created by fj on 2017/6/14.
 */
@Repository
public class LogTabDDLRepo extends MetadataBaseRepo<LogTabDDLEntity>  {
    public LogTabDDLRepo() {
        super(LogTabDDLEntity.class);
    }

    /**
     * 查询某个任务批次产生的DDL语句
     *
     * @param taskId
     * @return
     */
    @Transactional(readOnly = true)
    public List<LogTabDDLEntity> queryByTaskId(String taskId) {
        Map<String, Object> where = new HashMap<>();
        where.put("taskId", taskId);
        List<LogTabDDLEntity> entities = select(where);
        return entities;
    }


    @Transactional(readOnly = false)
    public Integer deleteByTaskId(String taskId) {
        Set<String> keys = new HashSet<>();
        keys.addAll(Arrays.asList("dbName,tabName,seq".split(",")));

        Map<String, Object> updates = new HashMap<>();
        updates.put("isDeleted", true);

        Map<String, Object> wheres = new HashMap<>();
        wheres.put("taskId", taskId);
        wheres.put("isDeleted", false);

        return update(keys, updates, wheres);

    }


}