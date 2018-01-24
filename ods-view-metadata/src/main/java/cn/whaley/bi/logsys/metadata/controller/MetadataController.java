package cn.whaley.bi.logsys.metadata.controller;

/**
 * Created by fj on 2017/7/3.
 */

import cn.whaley.bi.logsys.metadata.entity.*;
import cn.whaley.bi.logsys.metadata.repository.*;
import cn.whaley.bi.logsys.metadata.service.ODSViewService;
import cn.whaley.bigdata.dw.HiveUtil;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/metadata")
public class MetadataController {
    public static final Logger LOG = LoggerFactory.getLogger(MetadataController.class);

    @Autowired
    ODSViewService odsViewService;

    @Autowired
    AppLogKeyFieldDescRepo appLogKeyFieldDescRepo;

    @Autowired
    AppLogSpecialFieldDescRepo appLogSpecialFieldDescRepo;

    @Autowired
    LogFileFieldDescRepo logFileFieldDescRepo;

    @Autowired
    LogFileTaskInfoRepo logFileTaskInfoRepo;

    @Autowired
    LogFileKeyFieldValueRepo logFileKeyFieldValueRepo;

    @Autowired
    LogBaseInfoRepo logBaseInfoRepo;
    @Autowired
    LogFieldTypeInfoRepo logFieldTypeInfoRepo;
    @Autowired
    BlackTableInfoRepo blackTableInfoRepo;

    //-------------------metadata.black_table_info--------------------------
    //查询黑名单表
    @RequestMapping(value = "/black_table_info/all", method = RequestMethod.GET, produces = {"application/json"})
    public List<BlackTableInfoEntity> getAllBlackTableInfo() {
        List<BlackTableInfoEntity> all = blackTableInfoRepo.findAll();
        return all;
    }

    //-------------------log_field_type_info--------------------------
    //查询所有 字段类型
    @RequestMapping(value = "/log_field_type_info/all", method = RequestMethod.GET, produces = {"application/json"})
    public List<LogFieldTypeInfoEntity> getAllFiledTypeInfo() {
        List<LogFieldTypeInfoEntity> all = logFieldTypeInfoRepo.findAll();
        return all;
    }



    //-------------------log_baseInfo--------------------------
    //查询所有
    @RequestMapping(value = "/log_baseInfo/all", method = RequestMethod.GET, produces = {"application/json"})
    public List<LogBaseInfoEntity> getAllLogBaseInfo() {
        List<LogBaseInfoEntity> all = logBaseInfoRepo.findAll();
        return all;
    }

    //-------------------applog_key_field_desc--------------------------
    //查询所有
    @RequestMapping(value = "/applog_key_field_desc/all", method = RequestMethod.GET, produces = {"application/json"})
    public List<AppLogKeyFieldDescEntity> getAllAppLogKeyFieldDesc() {
        List<AppLogKeyFieldDescEntity> all = appLogKeyFieldDescRepo.findAll();
        return all;
    }

    //-------------------applog_special_field_desc--------------------------
    //查询所有
    @RequestMapping(value = "/applog_special_field_desc/all", method = RequestMethod.GET, produces = {"application/json"})
    public List<AppLogSpecialFieldDescEntity> getAllAppLogSpecialFieldDesc() {
        List<AppLogSpecialFieldDescEntity> all = appLogSpecialFieldDescRepo.findAll();
        return all;
    }

    //-------------------logfile_key_field_value--------------------------
    //插入操作
    @RequestMapping(value = "/logfile_key_field_value", method = RequestMethod.PUT, produces = {"application/json"}, consumes = {"application/json"})
    public ResponseEntity putLogFileKeyFieldValue(@RequestBody List<LogFileKeyFieldValueEntity> entities) {
        ResponseEntity<Integer> retEntity = new ResponseEntity();
        try {
            Integer ret = logFileKeyFieldValueRepo.insert(entities);
            retEntity.setResult(ret);
            return retEntity;
        } catch (Throwable ex) {
            LOG.error("", ex);
            retEntity.setCode(-1);
            retEntity.setMessage(ex.getMessage());
            return retEntity;
        }
    }

    //删除操作,all代表所有
    @RequestMapping(value = "/logfile_key_field_value/{taskId}", method = RequestMethod.DELETE)
    public ResponseEntity deleteLogFileKeyFieldValue(@PathVariable("taskId") String taskId) {
        ResponseEntity<Integer> retEntity = new ResponseEntity();
        try {
            if (taskId.equalsIgnoreCase("all")) taskId = "";
            Integer ret = logFileKeyFieldValueRepo.delete(taskId);
            retEntity.setResult(ret);
            return retEntity;
        } catch (Throwable ex) {
            LOG.error("", ex);
            retEntity.setCode(-1);
            retEntity.setMessage(ex.getMessage());
            return retEntity;
        }
    }

    //-------------------logfile_field_desc--------------------------
    //插入操作
    @RequestMapping(value = "/logfile_field_desc", method = RequestMethod.PUT, produces = {"application/json"}, consumes = {"application/json"})
    public ResponseEntity putLogFileFieldDesc(@RequestBody List<LogFileFieldDescEntity> entities) {
        ResponseEntity<Integer> retEntity = new ResponseEntity();
        try {
            Integer ret = logFileFieldDescRepo.insert(entities);
            retEntity.setResult(ret);
            return retEntity;
        } catch (Throwable ex) {
            LOG.error("", ex);
            retEntity.setCode(-1);
            retEntity.setMessage(ex.getMessage());
            return retEntity;
        }
    }

    //删除操作,all代表所有
    @RequestMapping(value = "/logfile_field_desc/{taskId}", method = RequestMethod.DELETE)
    public ResponseEntity deleteLogFileFieldDesc(@PathVariable("taskId") String taskId) {
        ResponseEntity<Integer> retEntity = new ResponseEntity();
        try {
            if (taskId.equalsIgnoreCase("all")) taskId = "";
            Integer ret = logFileFieldDescRepo.delete(taskId);
            retEntity.setResult(ret);
            return retEntity;
        } catch (Throwable ex) {
            LOG.error("", ex);
            retEntity.setCode(-1);
            retEntity.setMessage(ex.getMessage());
            return retEntity;
        }
    }

    //-------------------logfile_task_info--------------------------
    //插入
    @RequestMapping(value = "/logfile_task_info", method = RequestMethod.PUT, produces = {"application/json"}, consumes = {"application/json"})
    public ResponseEntity putLogFileTaskInfo(@RequestBody List<LogFileTaskInfoEntity> entities) {
        ResponseEntity<Integer> retEntity = new ResponseEntity();
        try {
            Integer ret = logFileTaskInfoRepo.insert(entities);
            retEntity.setResult(ret);
            return retEntity;
        } catch (Throwable ex) {
            LOG.error("", ex);
            retEntity.setCode(-1);
            retEntity.setMessage(ex.getMessage());
            return retEntity;
        }
    }


    //-------------------processTask--------------------------
    //处理某批任务
    @RequestMapping(value = "/processTask/{taskId}/{deleteOld}", method = RequestMethod.POST, produces = {"application/json"})
    @ResponseBody
    public ResponseEntity processTask(@PathVariable("taskId") String taskId, @PathVariable("deleteOld") String deleteOld) {

        Boolean disableGenerateDDLAndDML = false ;// taskFlag.charAt(0) == '0';
        Boolean disableExecuteDDL = false ; // taskFlag.charAt(1) == '0';
        Boolean disableExecuteDML = false ; //taskFlag.charAt(2) == '0';

        ResponseEntity<JSONObject> retEntity = new ResponseEntity();
        JSONObject result = new JSONObject();
        result.put("taskId", taskId);
        retEntity.setResult(result);

        if (!StringUtils.hasText(taskId)) {
            retEntity.setCode(-1);
            retEntity.setMessage("taskId required!");
            return retEntity;
        }
        LOG.info("task started.deleteOld={},taskFlag={}", taskId, deleteOld);
        Long fromTs = System.currentTimeMillis();
        try {

            if (!disableGenerateDDLAndDML) {
                Integer[] ddlAndDMLRet = odsViewService.generateDDLAndDML(taskId);
                Integer ret = Arrays.stream(ddlAndDMLRet).collect(Collectors.summingInt(item -> item));
                result.put("generateDDLAndDML", ret);
            }
            if (!disableExecuteDDL) {
                Integer ddlRet = odsViewService.executeDDL(taskId);
                result.put("executeDDL", ddlRet);
            }
            if (!disableExecuteDML) {
                Integer dmlRet = odsViewService.executeDML(taskId,deleteOld);
                result.put("executeDML", dmlRet);
            }
            retEntity.setCode(0);
            retEntity.setMessage("OK");
            Integer ddlSqlRet = odsViewService.executeSql(taskId);
            LOG.info("task ddlSqlRet={}", ddlSqlRet);
        } catch (Throwable ex) {
            LOG.error("taskId=" + taskId, ex);
            retEntity.setCode(-1);
            retEntity.setMessage(ex.getMessage());
        }
        LOG.info("task end.taskId={},deleteOld={}.ts={}", taskId, deleteOld, System.currentTimeMillis() - fromTs);
        return retEntity;

    }


}

