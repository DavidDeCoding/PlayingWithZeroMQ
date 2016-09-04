import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;

import java.nio.charset.Charset;
import java.util.HashMap;

public class ZSNRService extends Thread {
    private static final int OP = 0;
    private static final int VAR = 1;
    private static final int SERVICE = 0;
    private static final int DESTINATION = 1;

    private HashMap<String, String> services;
    private ZMQ.Socket router;

    public ZSNRService(String ip, ZContext ctx) {
        router = ctx.createSocket(ZMQ.ROUTER);
        services = new HashMap<>();
        router.bind("tcp://" + ip + ":9999");
    }

    public ZSNRService(ZContext ctx) {
        router = ctx.createSocket(ZMQ.ROUTER);
        services = new HashMap<>();
        router.bind("tcp://" + System.getenv("RABBITMQ_HOST") + ":9999");
    }

    public void run() {
        // Running server
        while (!Thread.interrupted()) {
            ZFrame id = ZFrame.recvFrame(router);
            String request = router.recvStr(Charset.defaultCharset());

            // There can be two types of request:
            String[] ops = request.split(" ");
            String response = "NA";


            if (ops[OP].equals("PUT"))
            {
                // 1. PUT servicename#ipaddress:port        -> To register
                String[] vars = ops[VAR].split("#");
                services.put(vars[SERVICE], vars[DESTINATION]);
                response = "OK";

            }
            else if (ops[OP].equals("GET"))
            {
                // 2. GET servicename                       -> To retrieve
                if (services.containsKey(ops[VAR])) response = services.get(ops[VAR]);
            }

            // Sending back response!
            id.send(router, ZMQ.SNDMORE);
            router.send(response);
        }
    }

    public static void main(String[] args) {
        ZContext ctx = new ZContext();

        ZSNRService service = new ZSNRService("*", ctx);
        service.start();

        ZMQ.Socket requester = ctx.createSocket(ZMQ.DEALER);
        requester.connect("tcp://127.0.0.1:9999");

        requester.send("PUT Persist#127.0.0.1:9998");
        System.out.println(requester.recvStr(Charset.defaultCharset()));

        requester.send("GET Persist");
        System.out.println(requester.recvStr(Charset.defaultCharset()));

        try { Thread.sleep(100000); } catch (Exception ex) {}
    }
}
