package cn.whaley.bi.logsys.metadata.repository;


import cn.whaley.bi.logsys.metadata.entity.AppLogKeyFieldDescEntity;
import cn.whaley.bi.logsys.metadata.entity.AppLogSpecialFieldDescEntity;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Created by fj on 2017/6/14.
 */

@Repository
public class AppLogSpecialFieldDescRepo extends MetadataBaseRepo<AppLogSpecialFieldDescEntity> {

    public AppLogSpecialFieldDescRepo() {
        super(AppLogSpecialFieldDescEntity.class);
    }

    public List<AppLogSpecialFieldDescEntity> findAll() {
        return selectAll();
    }

}
