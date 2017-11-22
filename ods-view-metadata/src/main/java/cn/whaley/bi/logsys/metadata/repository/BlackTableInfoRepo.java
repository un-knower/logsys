package cn.whaley.bi.logsys.metadata.repository;

import cn.whaley.bi.logsys.metadata.entity.BlackTableInfoEntity;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Created by guohao on 2017/10/18.
 *
 */
@Repository
public class BlackTableInfoRepo extends MetadataBaseRepo<BlackTableInfoEntity> {
    public BlackTableInfoRepo(){
        super(BlackTableInfoEntity.class);
    }
    public List<BlackTableInfoEntity> findAll() {
        return selectAll();
    }

}
