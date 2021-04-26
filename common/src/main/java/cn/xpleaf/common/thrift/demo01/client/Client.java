package cn.xpleaf.common.thrift.demo01.client;

import cn.xpleaf.common.thrift.demo01.service.QueryService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class Client {

    private static final String HOST = "localhost";

    private static final int PORT = 4893;

    private static final int TIMEOUT = 30000;

    public static void main(String[] args) {

        try (TTransport transport = new TSocket(HOST, PORT, TIMEOUT)) {
            // 协议要和服务端一致
            TProtocol protocol = new TBinaryProtocol(transport);
            // TProtocol protocol = new TCompactProtocol(transport);
            // TProtocol protocol = new TJSONProtocol(transport);
            QueryService.Client client = new QueryService.Client(protocol);
            transport.open();
            String result = client.query("xpleaf");
            System.out.println("Thrify client result =: " + result);
        } catch (TException e) {
            e.printStackTrace();
        }
    }
}
