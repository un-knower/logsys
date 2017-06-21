package cn.whaley.bi.logsys.metadata.repository;

import cn.whaley.bi.logsys.metadata.entity.BaseTableEntity;
import cn.whaley.bi.logsys.metadata.entity.SeqEntity;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by fj on 2017/6/14.
 */
public class MetadataBaseRepo<T extends BaseTableEntity> {

    /**
     * 记录集到实体对象映射器
     */
    EntityMapper<T> mapper;

    public MetadataBaseRepo(Class<T> clazz) {
        mapper = new EntityMapper(clazz);
    }

    @Resource(name = "metadataJdbcTemplate")
    protected JdbcTemplate jdbcTemplate;

    /**
     * 获取特定计数器的下一个值
     *
     * @param seqName
     * @return
     */
    public Integer getNextSeq(String seqName) {
        String sql = "SELECT NEXT VALUE FOR " + seqName;
        Integer ret = jdbcTemplate.queryForObject(sql, Integer.class);
        return ret;
    }


    /**
     * 原生方式查询符合条件的对象列表
     *
     * @param sql  SQL语句
     * @param args 参数列表
     * @return
     */
    public <R> List<R> query(String sql, EntityMapper<R> mapper, Object... args) {
        return jdbcTemplate.query(sql, mapper, args);
    }

    /**
     * 查询所有对象
     *
     * @return
     */
    public List<T> selectAll() {
        String selectSql = buildSelectSQL(mapper.tab_name);
        return jdbcTemplate.query(selectSql, mapper);
    }

    /**
     * 查询符合条件的对象列表
     *
     * @param where 参数列表
     * @return
     */
    public List<T> select(Map<String, Object> where) {
        String selectSql = buildSelectSQL(mapper.tab_name, where.keySet());
        Object[] params = where.values().toArray();
        return jdbcTemplate.query(selectSql, mapper, params);
    }


    /**
     * 通用插入方法
     *
     * @param entities
     * @return
     */
    @Transactional(readOnly = false)
    public Integer insert(List<T> entities) {
        String tabName = mapper.tab_name;
        Set<String> fieldNames = this.mapper.getFieldNames();
        String sql = buildInsertSQL(tabName, fieldNames);
        List<Object[]> params = new ArrayList<>();
        for (T entity : entities) {
            //填充自增序列值
            if (entity instanceof SeqEntity && ((SeqEntity) entity).getSeq() == null) {
                Integer seq = this.getNextSeq(((SeqEntity) entity).getSeqName());
                ((SeqEntity) entity).setSeq(seq);
            }
            Object[] rowParams = this.mapper.getFieldValues(entity).toArray();
            params.add(rowParams);
        }
        int[] values = jdbcTemplate.batchUpdate(sql, params);
        return Arrays.stream(values).sum();
    }

    /**
     * 通用更新方法
     *
     * @param keys    主键字段清单
     * @param updates 需要更新的字段及其目标值
     * @param wheres  条件限定字段及其目标值
     * @return
     */
    @Transactional(readOnly = false)
    public Integer update(Set<String> keys, Map<String, Object> updates, Map<String, Object> wheres) {

        String tabName = this.mapper.tab_name;

        Collection<String> allField = new ArrayList<>();
        allField.addAll(keys);
        allField.addAll(updates.keySet());
        allField = allField.stream().distinct().collect(Collectors.toList());

        List<T> entities = select(wheres);
        List<Object[]> batchParams = new ArrayList<>();
        Map<String, Object> rowKeys = new HashMap<>();
        for (T entity : entities) {
            rowKeys.clear();
            for (String key : keys) {
                rowKeys.put(key, mapper.getFieldValue(entity, key));
            }
            Object[] rowParams = allField.stream()
                    .map(field -> {
                        Object value = rowKeys.containsKey(field) ? rowKeys.get(field) : updates.get(field);
                        return value;
                    }).collect(Collectors.toList()).toArray();
            batchParams.add(rowParams);
        }
        String updateSql = buildInsertSQL(tabName, allField);
        int[] values = jdbcTemplate.batchUpdate(updateSql, batchParams);
        return Arrays.stream(values).sum();

    }


    /**
     * 构建insert语句
     *
     * @param tabName
     * @param colName
     * @return
     */
    String buildInsertSQL(String tabName, Collection<String> colName) {
        String sql = "upsert into " + tabName + " (" + colName.stream().collect(Collectors.joining(",")) + ")"
                + " values(" + StringUtils.repeat("?", ",", colName.size()) + ")";
        ;
        return sql;
    }

    String buildSelectSQL(String tabName, Collection<String> where) {
        String whereClause = where.stream().map(item -> item + " = ? ").collect(Collectors.joining(" and "));
        String sql = "select * from " + tabName + " where " + whereClause;
        return sql;
    }

    String buildSelectSQL(String tabName) {
        String sql = "select * from " + tabName;
        return sql;
    }

}
