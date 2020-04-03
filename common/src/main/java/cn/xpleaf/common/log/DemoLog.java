package cn.xpleaf.common.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 日志记录器（Logger）的行为是分等级的。
 * 分为OFF、FATAL、ERROR、WARN、INFO、DEBUG、ALL或者您定义的级别。
 * Log4j建议只使用四个级别，优先级从高到低分别是 ERROR、WARN、INFO、DEBUG。
 * 通过在这里定义的级别，您可以控制到应用程序中相应级别的日志信息的开关。
 * 比如在这里定义了INFO级别， 则应用程序中所有DEBUG级别的日志信息将不被打印出来
 */
public class DemoLog {

    // 没有指定获取logger的名字，默认都是使用log4j2.xml中配置的Root Logger
    private static final Logger LOGGER = LoggerFactory.getLogger(DemoLog.class);
    private static final Logger LOGGER2 = LoggerFactory.getLogger(DemoLog.class);
    // 指定获取logger的名字，使用Loggers中配置的特定logger
    private static final Logger LOGGER_MONITOR = LoggerFactory.getLogger("monitor");

    public static void main(String[] args) {
        LOGGER.info("just to the console!");
        LOGGER2.info("just to the console too!");
        LOGGER_MONITOR.info("just to the file");
    }

}
