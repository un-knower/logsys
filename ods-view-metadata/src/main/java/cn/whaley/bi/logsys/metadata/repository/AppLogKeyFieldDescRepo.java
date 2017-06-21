package cn.whaley.bi.logsys.metadata.repository;


import cn.whaley.bi.logsys.metadata.entity.AppLogKeyFieldDescEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by fj on 2017/6/14.
 */

@Repository
public class AppLogKeyFieldDescRepo extends MetadataBaseRepo<AppLogKeyFieldDescEntity> {

    public AppLogKeyFieldDescRepo() {
        super(AppLogKeyFieldDescEntity.class);
    }

    public List<AppLogKeyFieldDescEntity> findAll() {
        return selectAll();
    }

}
