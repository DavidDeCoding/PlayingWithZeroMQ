import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.charset.Charset;
import java.text.Collator;
import java.util.*;

/**
 * Created by daviddecoding on 8/25/16.
 */
public class PowerSort {
    public static void main(String[] args) {
        ZContext ctx = new ZContext();
        String driverSock = "inproc://driversock";
        int noOfFes = 2;
        int noOfBes = 2;

        // Driver(PUSH) --> PULL -> REQ -> LoadBalancer(ROUTER) --> REQ -> PUSH --> Driver(PULL)
        // Creating Driver
        Driver driver = new Driver(ctx, driverSock);

        // Creating the Load Balancer
        LoadBalancer lb = new LoadBalancer(ctx);

        // Creating frontend workers
        List<Object> fes = new ArrayList<>(noOfFes);
        for (int i = 0; i < noOfFes; i++) {
            FrontEnd fe = new FrontEnd(ctx);
            fe.connect(lb);
            fes.add(fe);
        }
        driver.connect(fes);

        // Creating backend workers
        List<Object> bes = new ArrayList<>(noOfBes);
        for (int i = 0; i < noOfBes; i++) {
            BackEnd be = new BackEnd(ctx);
            be.connect(lb);
            bes.add(be);
        }
        driver.connect(bes);


        // Lets do it!!!
        driver.start();
        lb.start();
        for (Object fe: fes) {
            ((FrontEnd) fe).start();
        }
        for (Object be: bes) {
            ((BackEnd) be).start();
        }


        // Client(PAIR) -> Driver(PAIR)
        ZMQ.Socket client = ctx.createSocket(ZMQ.PAIR);
        client.connect(driverSock);

        while (true) {
            Scanner scan = new Scanner(System.in);
            System.out.print("> ");
            String line = scan.nextLine();
            if (line.isEmpty()) continue;

            client.send(line);
            String result = client.recvStr(Charset.defaultCharset());
            System.out.println();
            System.out.println();
            System.out.println("===========================================================================================");
            System.out.println(result);
            System.out.println();
        }
    }



    // THE DRIVER
    static class Driver extends Thread {
        final String pushSocket = "inproc://driverpush";
        final String pullSocket = "inproc://driverpull";

        private ZMQ.Socket server;
        private ZMQ.Socket push;
        private ZMQ.Socket pull;

        Driver(ZContext ctx, String serverSocket) {
            server = ctx.createSocket(ZMQ.PAIR);
            push = ctx.createSocket(ZMQ.PUSH);
            pull = ctx.createSocket(ZMQ.PULL);

            // Initializing connections
            server.bind(serverSocket);
            push.bind(pushSocket);
            pull.bind(pullSocket);

        }

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                String[] instruction = server.recvStr(Charset.defaultCharset()).split(" ");

                // The Command Received...
                String operation = instruction[0];
                String argument = instruction[1];
                System.out.println("Performing operation " + operation + " on " + argument);
                System.out.println("===========================================================================================");

                // Parsing Request
                if (operation.equals("SORT")) {
                    try {
                        File file =  new File(argument);

                        if (file.exists()) {

                            if (file.isDirectory()) {
                                File[] internalFiles = file.listFiles();
                                for (File internalFile: internalFiles) {
                                    StringBuilder buff = new StringBuilder();
                                    BufferedReader reader = new BufferedReader(new FileReader(internalFile));
                                    String line = null;
                                    while ((line = reader.readLine()) != null) {
                                        buff.append(line);
                                    }
                                    push.send(buff.toString());
                                }
                                for (int responseCount = 0; responseCount < internalFiles.length; responseCount++) {
                                    // Waiting for result
                                    String[] result = pull.recvStr(Charset.defaultCharset()).split(" ");
                                    int count = 10;
                                    for (int i = 0; i < result.length; i++) {
                                        if (i % count == 0) System.out.print("\n");
                                        System.out.print(" " + result[i] + " ");
                                    }
                                }
                            }
                            else if (file.isFile()) {
                                StringBuilder buff = new StringBuilder();
                                BufferedReader reader = new BufferedReader(new FileReader(file));
                                String line = null;
                                while ((line = reader.readLine()) != null) {
                                    buff.append(line);
                                }
                                push.send(buff.toString());
                                // Waiting for result
                                String[] result = pull.recvStr(Charset.defaultCharset()).split(" ");
                                int count = 13;
                                for (int i = 0; i < result.length; i++) {
                                    if (i % count == 0) System.out.print("\n");
                                    System.out.print(" " + result[i] + " ");
                                }
                            }
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else if (operation.equals("USORT")) {
                    try {
                        File file =  new File(argument);

                        if (file.exists()) {

                            if (file.isDirectory()) {
                                File[] internalFiles = file.listFiles();
                                for (File internalFile: internalFiles) {
                                    StringBuilder buff = new StringBuilder();
                                    BufferedReader reader = new BufferedReader(new FileReader(internalFile));
                                    String line = null;
                                    while ((line = reader.readLine()) != null) {
                                        buff.append(line);
                                    }
                                    push.send(buff.toString());
                                }
                                for (int responseCount = 0; responseCount < internalFiles.length; responseCount++) {
                                    // Waiting for result
                                    List<String> words = Arrays.asList(pull.recvStr(Charset.defaultCharset()).split(" "));
                                    Set<String> set = new HashSet<String>();
                                    int count = 13;
                                    int i = 0;
                                    for (String word: words) {
                                        if (set.contains(word)) continue;

                                        i++;
                                        if (i % count == 0) System.out.print("\n");
                                        System.out.print(" " + word + " ");
                                        set.add(word);
                                    }
                                }
                            }
                            else if (file.isFile()) {
                                StringBuilder buff = new StringBuilder();
                                BufferedReader reader = new BufferedReader(new FileReader(file));
                                String line = null;
                                while ((line = reader.readLine()) != null) {
                                    buff.append(line);
                                }
                                push.send(buff.toString());
                                // Waiting for result
                                List<String> words = Arrays.asList(pull.recvStr(Charset.defaultCharset()).split(" "));
                                Set<String> set = new HashSet<String>();
                                int count = 13;
                                int i = 0;
                                for (String word: words) {
                                    if (set.contains(word)) continue;

                                    i++;
                                    if (i % count == 0) System.out.print("\n");
                                    System.out.print(" " + word + " ");
                                    set.add(word);
                                }
                            }
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                // Responding with result
                server.send("Done.");
            }


        }

        public void connect(List<Object> endpoints) {
            // Connecting the frontend with push socket and backend with pull socket
            for (Object endpoint: endpoints) {
                if (endpoint instanceof FrontEnd)   ((FrontEnd) endpoint).connectToDriver(pushSocket);
                else                                ((BackEnd) endpoint).connectToDriver(pullSocket);
            }
        }
    }



    // THE FRONTEND WORKER
    static class FrontEnd extends Thread {
        private ZMQ.Socket pull;
        private ZMQ.Socket req;

        FrontEnd(ZContext ctx) {
            pull = ctx.createSocket(ZMQ.PULL);
            req = ctx.createSocket(ZMQ.REQ);
            req.setIdentity(UUID.randomUUID().toString().getBytes());
        }

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                // Receiving data from driver
                String data = pull.recvStr(Charset.defaultCharset());

                // Sending data to load balancer
                req.send(data);

                // Waiting for data from load balancer
                String result = req.recvStr(Charset.defaultCharset());
            }

        }

        void connectToDriver(String pullAddress) {
            pull.connect(pullAddress);
        }

        void connect(LoadBalancer lb) {
            req.connect(lb.getFrontendSocket());
        }
    }


    // THE BACKEND WORKER
    static class BackEnd extends Thread {
        private ZMQ.Socket push;
        private ZMQ.Socket req;

        BackEnd(ZContext ctx) {
            push = ctx.createSocket(ZMQ.PUSH);
            req = ctx.createSocket(ZMQ.REQ);
            req.setIdentity(UUID.randomUUID().toString().getBytes());
        }

        @Override
        public void run() {
            // Sending hello to load balancer
            req.send("Available");

            while (!Thread.currentThread().isInterrupted()) {
                String frontEndAddress = req.recvStr(Charset.defaultCharset());
                String empty = req.recvStr(Charset.defaultCharset());
                assert (empty.length() == 0);

                // Receive data from load balancer
                String[] data = req.recvStr(Charset.defaultCharset()).split(" ");

                // Sending conformation back to load balancer for next task
                req.sendMore(frontEndAddress);
                req.sendMore("");
                req.send("Done");

                // Processing
                Arrays.sort(data, Collator.getInstance());
                StringBuilder result = new StringBuilder();
                for (int i = 0; i < data.length; i++) result.append(" " + data[i] + " ");

                // Send to push socket
                push.send(result.toString());
            }
        }

        void connectToDriver(String pushAddress) {
            push.connect(pushAddress);
        }

        void connect(LoadBalancer lb) {
            req.connect(lb.getBackendSocket());
        }
    }


    // THE LOADBALANCER
    private static class LoadBalancer extends Thread {
        private String frontendSocket;
        private String backendSocket;
        private ZMQ.Socket frontend;
        private ZMQ.Socket backend;

        LoadBalancer(ZContext ctx) {
            frontendSocket = "inproc://routerFrontEnd";
            backendSocket = "inproc://routerBackEnd";

            // Initializing sockets
            frontend = ctx.createSocket(ZMQ.ROUTER);
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
                    String frontEndAddress = backend.recvStr(Charset.defaultCharset());

                    // If frontend address, send ack back to frontend
                    if (!frontEndAddress.equals("Available")) {
                        empty = backend.recvStr(Charset.defaultCharset());
                        assert (empty.length() == 0);

                        String reply = backend.recvStr(Charset.defaultCharset());
                        frontend.sendMore(frontEndAddress);
                        frontend.sendMore("");
                        frontend.send("Done");
                    }
                }

                if (items.pollin(1)) {
                    String frontEndAddress = frontend.recvStr(Charset.defaultCharset());

                    String empty = frontend.recvStr(Charset.defaultCharset());
                    assert (empty.length() == 0);

                    String request = frontend.recvStr(Charset.defaultCharset());

                    String backendAddress = backendQueue.poll();

                    backend.sendMore(backendAddress);
                    backend.sendMore("");
                    backend.sendMore(frontEndAddress);
                    backend.sendMore("");
                    backend.send(request);
                }
            }
        }

        public String getFrontendSocket() {
            return frontendSocket;
        }

        public String getBackendSocket() {
            return backendSocket;
        }
    }
}
