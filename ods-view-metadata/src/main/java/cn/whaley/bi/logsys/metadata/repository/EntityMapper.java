package cn.whaley.bi.logsys.metadata.repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;

import java.lang.reflect.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by fj on 2017/6/19.
 */
public class EntityMapper<T> implements RowMapper<T> {

    public static Logger LOG = LoggerFactory.getLogger(EntityMapper.class);

    //实体对象类型
    Class clazz;

    //实体类型字段及字段类型
    Map<String, Class<?>> fieldTypeMap = new HashMap<>();

    //实体类型属性字段及其Set方法
    Map<String, Method> fieldSetMap = new HashMap<>();

    //实体类型属性字段及其Get方法
    Map<String, Method> fieldGetMap = new HashMap<>();


    //表名
    public final String tab_name;

    //自增器名
    public final String seq_name;

    public EntityMapper(Class<T> clazz) {
        this.clazz = clazz;


        Field tabNameField = null;
        Field seqNameField = null;
        Field[] field = clazz.getDeclaredFields();
        for (int j = 0; j < field.length; j++) {
            String name = field[j].getName();
            if (name.equals("TABLE_NAME")) {
                tabNameField = field[j];
                continue;
            } else if (name.equals("SEQ_NAME")) {
                seqNameField = field[j];
                continue;
            }
        }
        if (tabNameField != null) {
            try {
                tab_name = tabNameField.get(clazz).toString();
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        } else {
            tab_name = "";
        }
        if (seqNameField != null) {
            try {
                seq_name = seqNameField.get(clazz).toString();
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        } else {
            seq_name = "";
        }

        init();

    }

    /**
     * 获取Set字段列表
     *
     * @return
     */
    public Set<String> getFieldNames() {
        return fieldSetMap.keySet();
    }

    /**
     * 获取所有字段值
     *
     * @return
     */
    public List<Object> getFieldValues(T entity) {
        List<Object> values = fieldSetMap.keySet().stream()
                .map(fieldName -> getFieldValue(entity, fieldName))
                .collect(Collectors.toList());
        return values;
    }

    /**
     * 获取字段值
     *
     * @param entity
     * @param fieldName
     * @return
     */
    public Object getFieldValue(T entity, String fieldName) {
        try {
            Object value = fieldGetMap.get(fieldName).invoke(entity);
            return value;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public T mapRow(ResultSet rs, int rowNum) throws SQLException {
        try {
            T entity = (T) (clazz.newInstance());
            fillEntity(entity, rs, rowNum, null);
            return entity;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    void init() {
        Method[] allMethods = clazz.getMethods();

        //set
        Map<String, Method> setMethods = new HashMap<>();
        Arrays.asList(allMethods).stream()
                .filter(method -> method.getName().startsWith("set"))
                .forEach(method -> {
                    setMethods.put(method.getName().substring(3), method);
                });

        //get
        Map<String, Method> getMethods = new HashMap<>();
        Arrays.asList(allMethods).stream()
                .filter(method -> method.getName().startsWith("get"))
                .forEach(method -> {
                    getMethods.put(method.getName().substring(3), method);
                });

        //is
        Map<String, Method> isMethods = new HashMap<>();
        Arrays.asList(allMethods).stream()
                .filter(method -> method.getName().startsWith("is"))
                .forEach(method -> {
                    isMethods.put(method.getName(), method);
                });


        List<Method> setMethodList = setMethods.keySet().stream()
                .filter(propName -> {
                    Boolean exists = getMethods.keySet().contains(propName);
                    exists = exists || isMethods.keySet().contains(propName.substring(0, 1).toLowerCase() + propName.substring(1));
                    return exists;
                })
                .map(propName -> setMethods.get(propName))
                .collect(Collectors.toList());

        List<Method> getMethodList = getMethods.keySet().stream()
                .filter(propName -> setMethods.keySet().contains(propName))
                .map(propName -> getMethods.get(propName))
                .collect(Collectors.toList());

        List<Method> isMethodList = isMethods.keySet().stream()
                .filter(propName -> {
                    Boolean exists = setMethods.keySet().contains(propName.substring(0, 1).toUpperCase() + propName.substring(1));
                    exists = exists || setMethods.keySet().contains(propName.substring(2));
                    return exists;
                })
                .map(propName -> isMethods.get(propName))
                .collect(Collectors.toList());

        setMethodList.stream().forEach(method -> {
            String fullName = method.getName();
            String fieldName = fullName.substring(3, 4).toLowerCase() + fullName.substring(4, 5).toLowerCase() + fullName.substring(5);
            fieldSetMap.put(fieldName, method);
        });

        //is属性合并到get中
        isMethodList.stream().forEach(method -> {
            String fullName = method.getName();
            String fieldName = fullName;
            fieldGetMap.put(fieldName, method);
        });

        getMethodList.stream().forEach(method -> {
            String fullName = method.getName();
            String fieldName = fullName.substring(3, 4).toLowerCase() + fullName.substring(4, 5).toLowerCase() + fullName.substring(5);
            fieldGetMap.put(fieldName, method);
        });

        for (Map.Entry<String, Method> entry : fieldGetMap.entrySet()) {
            String fieldName = entry.getKey();
            Method method = entry.getValue();
            Class<?> returnType = method.getReturnType();
            fieldTypeMap.put(fieldName, returnType);
        }

        /*
        Field[] field = clazz.getDeclaredFields();

        for (int j = 0; j < field.length; j++) {
            String name = field[j].getName();
            Class type = field[j].getType();
            fieldTypeMap.put(name, type);
            //get+set
            try {
                String setMethodName = "set" + name.substring(0, 1).toUpperCase() + name.substring(1);
                Method setMethod = clazz.getMethod(setMethodName, type);
                fieldSetMap.put(name, setMethod);
                String getMethodName = "get" + name.substring(0, 1).toUpperCase() + name.substring(1);
                Method getMethod = clazz.getMethod(getMethodName);
                fieldGetMap.put(name, getMethod);

            } catch (Exception ex) {
                LOG.debug("ignore field setMethod: " + name + ",type:" + type.getName());
            }
        }
        */


    }

    void fillEntity(T entity, ResultSet rs, int rowNum, Set<String> fieldNames) throws Exception {
        if (fieldNames == null) {
            fieldNames = new HashSet<>();
            int count = rs.getMetaData().getColumnCount();
            for (int i = 1; i <= count; i++) {
                fieldNames.add(rs.getMetaData().getColumnName(i));
            }
        }

        for (Map.Entry<String, Class<?>> field : fieldTypeMap.entrySet()) {
            //rs字段名全部为大写
            String fieldName = field.getKey();
            Class<?> fieldType = field.getValue();
            Method m = fieldSetMap.get(fieldName);

            String rsFieldName = fieldName.toUpperCase();
            if (!fieldNames.contains(rsFieldName)) {
                continue;
            }

            Object value = rs.getObject(rsFieldName);
            if (value == null) {
                continue;
            }

            //做一些类型转换工作
            if (fieldType.getName().endsWith("java.lang.String")) {
                if (!(fieldType.isInstance(value))) {
                    value = value.toString();
                }
            } else if (fieldType.getName().endsWith("java.lang.Integer")) {
                if (!(fieldType.isInstance(value))) {
                    value = Integer.parseInt(value.toString());
                }
            } else if (fieldType.getName().endsWith("java.lang.Double")) {
                if (!(fieldType.isInstance(value))) {
                    value = Double.parseDouble(value.toString());
                }
            } else if (fieldType.getName().endsWith("java.lang.Boolean")) {
                if (!(fieldType.isInstance(value))) {
                    value = Boolean.parseBoolean(value.toString());
                }
            } else if (fieldType.getName().endsWith("java.util.Date")) {
                if (!(fieldType.isInstance(value))) {
                    value = new SimpleDateFormat().parse(value.toString());
                }
            }

            m.invoke(entity, value);
        }

    }

}
