package cn.whaley.bi.logsys.metadata.repository;

import cn.whaley.bi.logsys.metadata.entity.LogFieldTypeInfoEntity;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Created by guohao on 2017/10/18.
 *
 */
@Repository
public class LogFieldTypeInfoRepo extends MetadataBaseRepo<LogFieldTypeInfoEntity> {
    public LogFieldTypeInfoRepo(){
        super(LogFieldTypeInfoEntity.class);
    }
    public List<LogFieldTypeInfoEntity> findAll() {
        return selectAll();
    }

}
