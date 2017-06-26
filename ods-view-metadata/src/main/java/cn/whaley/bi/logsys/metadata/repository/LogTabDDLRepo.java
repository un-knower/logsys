package cn.whaley.bi.logsys.metadata.repository;

import cn.whaley.bi.logsys.metadata.entity.LogTabDDLEntity;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

/**
 * Created by fj on 2017/6/14.
 */
@Repository
public class LogTabDDLRepo extends MetadataBaseRepo<LogTabDDLEntity> {

    private static final Set<String> LOG_TAB_DDL_KEYOG_TAB_FIELDS;

    static {
        LOG_TAB_DDL_KEYOG_TAB_FIELDS = new HashSet<>();
        LOG_TAB_DDL_KEYOG_TAB_FIELDS.addAll(Arrays.asList("dbName,tabName,seq".split(",")));
    }

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
    public List<LogTabDDLEntity> queryByTaskId(String taskId,boolean isDeleted) {
        Map<String, Object> where = new HashMap<>();
        where.put("taskId", taskId);
        where.put("isDeleted", isDeleted);
        List<LogTabDDLEntity> entities = select(where);
        return entities;
    }


    @Transactional(readOnly = false)
    public Integer deleteByTaskId(String taskId) {

        Map<String, Object> updates = new HashMap<>();
        updates.put("isDeleted", true);

        Map<String, Object> wheres = new HashMap<>();
        wheres.put("taskId", taskId);
        wheres.put("isDeleted", false);

        return update(LOG_TAB_DDL_KEYOG_TAB_FIELDS, updates, wheres);

    }

    public Integer updateCommitInfo(LogTabDDLEntity entity) {

        Map<String, Object> updates = new HashMap<>();
        updates.put("commitTime", entity.getCommitTime());
        updates.put("commitCode", entity.getCommitCode());
        updates.put("commitMsg", entity.getCommitMsg());

        Map<String, Object> wheres = new HashMap<>();
        wheres.put("dbName", entity.getDbName());
        wheres.put("tabName", entity.getTabName());
        wheres.put("seq", entity.getSeq());

        return update(LOG_TAB_DDL_KEYOG_TAB_FIELDS, updates, wheres);
    }


}
