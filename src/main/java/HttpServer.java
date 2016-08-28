import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import java.nio.charset.Charset;
import java.text.Collator;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;
import java.util.UUID;

/**
 * Created by daviddecoding on 8/26/16.
 */
public class HttpServer {

    private static final int ZMQ_STREAM = 11;

    public static void main(String[] args) {
        final int noOfBackedWorkers = 1;
        ZContext ctx = new ZContext();

        LoadBalancer lb = new LoadBalancer(ctx);
        lb.start();

        for (int i = 0; i < noOfBackedWorkers; i++) {
            BackEnd be = new BackEnd(ctx);
            be.connect(lb);
            be.start();
        }

        System.out.println("The http server is running on 127.0.0.1:10001...");

        while (!Thread.interrupted()) {
            try {
                Thread.sleep(10);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        ctx.close();
    }

    static class BackEnd extends Thread {
        private ZMQ.Socket req;

        BackEnd(ZContext ctx) {
            req = ctx.createSocket(ZMQ.REQ);
            req.setIdentity(UUID.randomUUID().toString().getBytes());
        }

        @Override
        public void run() {
            // Sending hello to load balancer
            req.send("Available");

            while (!Thread.currentThread().isInterrupted()) {
                ZFrame frontEndAddress = ZFrame.recvFrame(req);
                String empty = req.recvStr(Charset.defaultCharset());
                assert (empty.length() == 0);

                // Receive data from load balancer
                String[] data = req.recvStr(Charset.defaultCharset()).split(" ");

                // Sending conformation back to load balancer for next task
                frontEndAddress.send(req, ZMQ.SNDMORE);
                req.sendMore("");
                req.send("<html>" +
                        "<head></head>" +
                        "<body style='margin: 0 0;background-image: url(\"http://fashion.tdprofiti.com/wp-content/plugins/RSSPoster_PRO/cache/8fccb_Marquis-Matrix.gif\");background-repeat: repeat;'></body>" +
                        "</html>");
            }
        }

        void connect(LoadBalancer lb) {
            req.connect(lb.getBackendSocket());
        }
    }

    private static class LoadBalancer extends Thread {
        private String frontendSocket;
        private String backendSocket;
        private ZMQ.Socket frontend;
        private ZMQ.Socket backend;

        LoadBalancer(ZContext ctx) {
            frontendSocket = "tcp://127.0.0.1:10001";
            backendSocket = "inproc://routerBackEnd";

            // Initializing sockets
            frontend = ctx.createSocket(ZMQ_STREAM);
            backend = ctx.createSocket(ZMQ.ROUTER);

            // Attaching sockets
            frontend.bind(frontendSocket);
            backend.bind(backendSocket);
        }

        @Override
        public void run() {
            Queue<String> backendQueue = new LinkedList<>();

            while (!Thread.currentThread().isInterrupted()) {
                ZMQ.Poller items = new ZMQ.Poller(2);

                // Polling for activity on backend!!!
                items.register(backend, ZMQ.Poller.POLLIN);

                // Polling for activity in the front end only if there are backend workers
                if (backendQueue.size() > 0) {
                    items.register(frontend, ZMQ.Poller.POLLIN);
                }

                if (items.poll() < 0) break;

                // Handling the polled items
                if (items.pollin(0)) {
                    // Add free backend to backendqueue!
                    backendQueue.add(backend.recvStr(Charset.defaultCharset()));

                    // Drop empty bullsh*t
                    String empty = backend.recvStr(Charset.defaultCharset());
                    assert (empty.length() == 0);

                    // The payload is 'Available' notification or frontend address!
                    ZFrame frontEndAddress = ZFrame.recvFrame(backend);

                    // If frontend address, send ack back to frontend
                    if (!frontEndAddress.toString().equals("Available")) {
                        empty = backend.recvStr(Charset.defaultCharset());
                        assert (empty.length() == 0);

                        String reply = backend.recvStr(Charset.defaultCharset());
                        int contentLength = reply.length();

                        frontEndAddress.send(frontend, ZMQ.SNDMORE);
                        frontend.send(
                                "HTTP/1.0 200 OK\r\n" +
                                "Date: Fri, 31 Dec 1999 23:59:59 GMT\r\n" +
                                "Content-Type: text/html\r\n" +
                                "Content-Length: " + contentLength + "\r\n" +
                                "\r\n" +
                                reply);
                    }
                }

                if (items.pollin(1)) {
                    ZFrame frontEndAddress = ZFrame.recvFrame(frontend);

                    String request = frontend.recvStr(Charset.defaultCharset());

                    if (!request.isEmpty()) {
                        String backendAddress = backendQueue.poll();

                        backend.sendMore(backendAddress);
                        backend.sendMore("");
                        frontEndAddress.send(backend, ZMQ.SNDMORE);
                        backend.sendMore("");
                        backend.send(request);
                    }
                }
            }
        }

        private String getBackendSocket() {
            return backendSocket;
        }
    }
}
