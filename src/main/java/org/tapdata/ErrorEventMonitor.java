package org.tapdata;


import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ErrorEventMonitor {
    public static final Logger logger = LoggerFactory.getLogger(ErrorEventMonitor.class);
    public static void main(String[] args) {
        if(args.length == 0){
            args = new String[]{"","",""};
        }
        String propFileName = StringUtils.isNotEmpty(System.getenv(ParameterEnum.TAPDATA_WORK_DIR.name())) ? System.getenv(ParameterEnum.TAPDATA_WORK_DIR.name()) : args[ParameterEnum.TAPDATA_WORK_DIR.getValue()];
        String mongoUri = StringUtils.isNotEmpty(System.getenv(ParameterEnum.TAPDATA_MONGODB_URI.name())) ? System.getenv(ParameterEnum.TAPDATA_MONGODB_URI.name()) : args[ParameterEnum.TAPDATA_MONGODB_URI.getValue()];
        String database = StringUtils.isNotEmpty(System.getenv(ParameterEnum.TAPDARA_DATABASE.name())) ? System.getenv(ParameterEnum.TAPDATA_WORK_DIR.name()) : args[ParameterEnum.TAPDARA_DATABASE.getValue()];
        if(StringUtils.isEmpty(propFileName)){
            logger.warn("Please set the TAPDATA_WORK_DIR environment variable or input parameter");
            System.exit(0);
        }else if(StringUtils.isEmpty(mongoUri)){
            logger.warn("Please set the TAPDATA_MONGODB_URI environment variable or input parameter");
            System.exit(0);
        }else if(StringUtils.isEmpty(database)){
            logger.warn("Please set the TAPDARA_DATABASE environment variable or input parameter");
            System.exit(0);
        }
        ExecutorService cachedThreadPool = Executors.newFixedThreadPool(1);
        cachedThreadPool.execute(new FileWatchTask(propFileName + File.separator+"logs"+ File.separator+"jobs",mongoUri,database));
    }


}