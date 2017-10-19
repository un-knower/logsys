package cn.whaley.bi.logsys.metadata.repository;

import cn.whaley.bi.logsys.metadata.entity.LogBaseInfoEntity;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Created by guohao on 2017/10/18.
 *
 */
@Repository
public class LogBaseInfoRepo extends MetadataBaseRepo<LogBaseInfoEntity> {
    public LogBaseInfoRepo(){
        super(LogBaseInfoEntity.class);
    }
    public List<LogBaseInfoEntity> findAll() {
        return selectAll();
    }

}
