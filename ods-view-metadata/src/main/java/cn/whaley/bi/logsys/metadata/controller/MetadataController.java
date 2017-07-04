package cn.whaley.bi.logsys.metadata.controller;

/**
 * Created by fj on 2017/7/3.
 */

import cn.whaley.bi.logsys.metadata.entity.*;
import cn.whaley.bi.logsys.metadata.repository.*;
import cn.whaley.bi.logsys.metadata.service.ODSViewService;
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

    //-------------------applog_key_field_desc--------------------------
    //查询所有
    @RequestMapping(value = "/applog_key_field_desc/get/all", method = RequestMethod.GET, produces = {"application/json"})
    public List<AppLogKeyFieldDescEntity> getAllAppLogKeyFieldDescList() {
        List<AppLogKeyFieldDescEntity> all = appLogKeyFieldDescRepo.findAll();
        return all;
    }

    //-------------------applog_special_field_desc--------------------------
    //查询所有
    @RequestMapping(value = "/applog_special_field_desc/get/all", method = RequestMethod.GET, produces = {"application/json"})
    public List<AppLogSpecialFieldDescEntity> getAllAppLogSpecialFieldDescList() {
        List<AppLogSpecialFieldDescEntity> all = appLogSpecialFieldDescRepo.findAll();
        return all;
    }

    //-------------------logfile_key_field_value--------------------------
    //插入操作
    @RequestMapping(value = "/logfile_key_field_value/insert", method = RequestMethod.POST, produces = {"application/json"}, consumes = {"application/json"})
    public ResponseEntity insertLogFileKeyFieldValue(@RequestBody List<LogFileKeyFieldValueEntity> entities) {
        ResponseEntity<Integer> retEntity = new ResponseEntity();
        try {
            Integer ret = logFileKeyFieldValueRepo.insert(entities);
            retEntity.setResult(ret);
            return retEntity;
        } catch (Throwable ex) {
            retEntity.setCode(-1);
            retEntity.setMessage(ex.getMessage());
            return retEntity;
        }
    }

    //删除操作,all代表所有
    @RequestMapping(value = "/logfile_key_field_value/{taskId}/{logPath}", method = RequestMethod.DELETE)
    public ResponseEntity deleteLogFileKeyFieldValue(@PathVariable("taskId") String taskId, @PathVariable("logPath") String logPath) {
        ResponseEntity<Integer> retEntity = new ResponseEntity();
        try {
            if (taskId.equalsIgnoreCase("all")) taskId = "";
            if (logPath.equalsIgnoreCase("all")) logPath = "";
            Integer ret = logFileKeyFieldValueRepo.delete(taskId, logPath);
            retEntity.setResult(ret);
            return retEntity;
        } catch (Throwable ex) {
            retEntity.setCode(-1);
            retEntity.setMessage(ex.getMessage());
            return retEntity;
        }
    }

    //-------------------logfile_field_desc--------------------------
    //插入操作
    @RequestMapping(value = "/logfile_field_desc/insert", method = RequestMethod.POST, produces = {"application/json"}, consumes = {"application/json"})
    public ResponseEntity insertLogFileFieldDesc(@RequestBody List<LogFileFieldDescEntity> entities) {
        ResponseEntity<Integer> retEntity = new ResponseEntity();
        try {
            Integer ret = logFileFieldDescRepo.insert(entities);
            retEntity.setResult(ret);
            return retEntity;
        } catch (Throwable ex) {
            retEntity.setCode(-1);
            retEntity.setMessage(ex.getMessage());
            return retEntity;
        }
    }

    //删除操作,all代表所有
    @RequestMapping(value = "/logfile_field_desc/{taskId}/{logPath}", method = RequestMethod.DELETE)
    public ResponseEntity deleteLogFileFieldDesc(@PathVariable("taskId") String taskId, @PathVariable("logPath") String logPath) {
        ResponseEntity<Integer> retEntity = new ResponseEntity();
        try {
            if (taskId.equalsIgnoreCase("all")) taskId = "";
            if (logPath.equalsIgnoreCase("all")) logPath = "";
            Integer ret = logFileFieldDescRepo.delete(taskId, logPath);
            retEntity.setResult(ret);
            return retEntity;
        } catch (Throwable ex) {
            retEntity.setCode(-1);
            retEntity.setMessage(ex.getMessage());
            return retEntity;
        }
    }

    //-------------------logfile_task_info--------------------------
    //插入
    @RequestMapping(value = "/logfile_task_info/insert", method = RequestMethod.POST, produces = {"application/json"}, consumes = {"application/json"})
    public ResponseEntity insertLogFileTaskInfo(@RequestBody List<LogFileTaskInfoEntity> entities) {
        ResponseEntity<Integer> retEntity = new ResponseEntity();
        try {
            Integer ret = logFileTaskInfoRepo.insert(entities);
            retEntity.setResult(ret);
            return retEntity;
        } catch (Throwable ex) {
            retEntity.setCode(-1);
            retEntity.setMessage(ex.getMessage());
            return retEntity;
        }
    }


    //-------------------processTask--------------------------
    //处理某批任务
    @RequestMapping(value = "/processTask/{taskId}/{taskFlag}", method = RequestMethod.POST, produces = {"application/json"})
    @ResponseBody
    public ResponseEntity processTask(@PathVariable("taskId") String taskId, @PathVariable("taskFlag") String taskFlag) {

        Boolean disableGenerateDDLAndDML = taskFlag.charAt(0) == '0';
        Boolean disableExecuteDDL = taskFlag.charAt(1) == '0';
        Boolean disableExecuteDML = taskFlag.charAt(2) == '0';

        ResponseEntity<JSONObject> retEntity = new ResponseEntity();
        JSONObject result = new JSONObject();
        result.put("taskId", taskId);
        retEntity.setResult(result);

        if (!StringUtils.hasText(taskId)) {
            retEntity.setCode(-1);
            retEntity.setMessage("taskId required!");
            return retEntity;
        }

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
                Integer dmlRet = odsViewService.executeDML(taskId);
                result.put("executeDML", dmlRet);
            }
            retEntity.setCode(1);
            retEntity.setMessage("OK");
        } catch (Throwable ex) {
            LOG.error("taskId=" + taskId, ex);
            retEntity.setCode(-1);
            retEntity.setMessage(ex.getMessage());
        }

        return retEntity;

    }


}

