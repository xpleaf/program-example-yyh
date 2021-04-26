package cn.xpleaf.common.thrift.demo01.server;

import cn.xpleaf.common.thrift.demo01.service.QueryService;
import org.apache.thrift.TException;

public class QueryServiceImpl implements QueryService.Iface {
    @Override
    public String query(String query) throws TException {
        return "Hello, " + query;
    }
}
