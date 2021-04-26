package cn.xpleaf.common.thrift.demo01.server;

import cn.xpleaf.common.thrift.demo01.service.QueryService;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;

import java.net.InetSocketAddress;

/**
 * QuickStart参考：
 * https://www.jianshu.com/p/d4123e8567c5
 *
 * 关于Thrift的更多原理和背景介绍：
 * https://www.zhihu.com/column/c_1338060193646198784
 */
public class Server {

    private static final String HOST = "localhost";

    private static final int PORT = 4893;

    private static final int TIMEOUT = 30000;

    public static void main(String[] args) {
        try {
            System.out.println("QueryService TSimpleServer start ....");

            TProcessor tprocessor = new QueryService.Processor<QueryService.Iface>(new QueryServiceImpl());
            // 简单的单线程服务模型，一般用于测试
            TServerSocket serverTransport = new TServerSocket(new InetSocketAddress(HOST, PORT), TIMEOUT);
            TServer.Args tArgs = new TServer.Args(serverTransport);
            tArgs.processor(tprocessor);
            tArgs.protocolFactory(new TBinaryProtocol.Factory());
            // tArgs.protocolFactory(new TCompactProtocol.Factory());
            // tArgs.protocolFactory(new TJSONProtocol.Factory());
            TServer server = new TSimpleServer(tArgs);
            server.serve();
        } catch (Exception e) {
            System.out.println("Server start error!!!");
            e.printStackTrace();
        }
    }

}