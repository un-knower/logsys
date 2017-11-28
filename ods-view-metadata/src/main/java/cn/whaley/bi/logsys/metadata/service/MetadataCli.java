package cn.whaley.bi.logsys.metadata.service;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.util.Assert;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by fj on 2017/7/3.
 */
public class MetadataCli implements CommandLineRunner {

    public static final Logger LOG = LoggerFactory.getLogger(MetadataCli.class);

    public static Map<String, String> parseArgs(String... args) {
        Map<String, String> params = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("--")) {
                Integer idx = arg.indexOf('=');
                String key = arg.substring(2, idx);
                String value = arg.substring(idx + 1);
                params.put(key, value);
            }
        }
        return params;
    }


    @Autowired
    ODSViewService odsViewService;

    /**
     * --taskId : taskId
     * --disableGenerateDDLAndDML:  true/false, 指示是否禁用GenerateDDLAndDML操作
     * --disableExecuteDDL:  true/false, 指示是否禁用ExecuteDLL操作
     * --disableExecuteDML:  true/false, 指示是否禁用ExecuteDML操作
     *
     * @param strings
     * @throws Exception
     */
    @Override
    public void run(String... strings) throws Exception {
        LOG.info("params:" + StringUtils.join(strings, " "));
        Map<String, String> params = parseArgs(strings);

        String taskId = params.get("taskId");
        String deleteOld = params.get("deleteOld");
        Boolean disableGenerateDDLAndDML = Boolean.parseBoolean(params.getOrDefault("disableGenerateDDLAndDML", "false"));
        Boolean disableExecuteDDL = Boolean.parseBoolean(params.getOrDefault("disableExecuteDDL", "false"));
        Boolean disableExecuteDML = Boolean.parseBoolean(params.getOrDefault("disableExecuteDML", "false"));

        Assert.hasText(taskId, "taskId required!");

        LOG.info("processing. taskId:" + taskId);

        if (!disableGenerateDDLAndDML) {
            Integer[] ddlAndDMLRet = odsViewService.generateDDLAndDML(taskId);
            LOG.info("generateDDLAndDML:" + Arrays.stream(ddlAndDMLRet).collect(Collectors.summingInt(item -> item)));
        }
        if (!disableExecuteDDL) {
            Integer ddlRet = odsViewService.executeDDL(taskId);
            LOG.info("executeDDL:" + ddlRet);
        }
        if (!disableExecuteDML) {
            Integer dmlRet = odsViewService.executeDML(taskId,deleteOld);
            LOG.info("executeDML:" + dmlRet);
        }

        LOG.info("done. taskId:" + taskId);

    }

}
