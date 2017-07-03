package cn.whaley.bi.logsys.metadata.controller;

/**
 * Created by fj on 2017/7/3.
 */

import cn.whaley.bi.logsys.metadata.entity.AppLogKeyFieldDescEntity;
import cn.whaley.bi.logsys.metadata.entity.AppLogSpecialFieldDescEntity;
import cn.whaley.bi.logsys.metadata.repository.AppLogKeyFieldDescRepo;
import cn.whaley.bi.logsys.metadata.repository.AppLogSpecialFieldDescRepo;
import cn.whaley.bi.logsys.metadata.service.ODSViewService;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
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


    @RequestMapping(value = "/applog_key_field_desc/all", produces = {"application/json"})
    public List<AppLogKeyFieldDescEntity> getAllAppLogKeyFieldDescList() {
        List<AppLogKeyFieldDescEntity> all = appLogKeyFieldDescRepo.findAll();
        return all;
    }

    @RequestMapping(value = "/applog_special_field_desc/all", produces = {"application/json"})
    public List<AppLogSpecialFieldDescEntity> getAllAppLogSpecialFieldDescList() {
        List<AppLogSpecialFieldDescEntity> all = appLogSpecialFieldDescRepo.findAll();
        return all;
    }

    @RequestMapping(value = "/processTask/{taskId}/{taskFlag}", method = RequestMethod.GET, produces = {"application/json"})
    public
    @ResponseBody
    JSONObject processTask(@PathVariable("taskId") String taskId, @PathVariable("taskFlag") String taskFlag) {

        Boolean disableGenerateDDLAndDML = taskFlag.charAt(0) == '0';
        Boolean disableExecuteDDL = taskFlag.charAt(1) == '0';
        Boolean disableExecuteDML = taskFlag.charAt(2) == '0';

        JSONObject retObj = new JSONObject();
        JSONObject result = new JSONObject();
        result.put("taskId", taskId);
        retObj.put("result", result);

        if (!StringUtils.hasText(taskId)) {
            retObj.put("code", -1);
            retObj.put("message", "taskId required!");
            return retObj;
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
            retObj.put("code", 1);
            retObj.put("message", "OK");
        } catch (Throwable ex) {
            LOG.error("taskId=" + taskId, ex);
            retObj.put("code", -1);
            retObj.put("message", ex.getMessage());
        }

        return retObj;

    }


}

