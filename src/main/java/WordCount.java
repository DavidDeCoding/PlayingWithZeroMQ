import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Created by daviddecoding on 8/24/16.
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        ZContext ctx = new ZContext();
        final String pushPullSocket = "inproc://pushPullSocket";
        final List<String> knownWords = Arrays.asList("Obama", "Hillary", "America", "ISIS", "Iran");
        final List<String> routerDealerSockets = Arrays.asList("inproc://routerDealer1", "inproc://routerDealer2");


        // Result Container
        Map<String, AtomicInteger> result = new ConcurrentHashMap<>();
        for (String word: knownWords) {
            result.put(word, new AtomicInteger(0));
        }

        // PUSH --> PULL -> ROUTER --> DEALER
        // --> means more than one or multiple.
        // -> means one or single.

        // 1. Initializing a push socket
        ZMQ.Socket push = ctx.createSocket(ZMQ.PUSH);
        push.bind(pushPullSocket);

        // 2. Initializing a Pull to Router actor
        for (String routerDealerSocket: routerDealerSockets) {
            new PullToRouterActor(ctx, routerDealerSocket, pushPullSocket, knownWords).start();
        }

        // 3. For every word we are initializing a Dealer actor
        for (String word: knownWords) {
            new DealerActor(ctx, routerDealerSockets, word, result).start();
        }


        // Testing
        BufferedReader reader = new BufferedReader(new FileReader("/Users/daviddecoding/IdeaProjects/ZMQ/data/trump_rnc_speech.txt"));
        String line;
        while ((line = reader.readLine()) != null)
        {
            push.send(line);
        }

        // TODO: New way to handle waits :(
        try { Thread.sleep(1000); } catch (Exception e) { e.printStackTrace(); } // Lets sleep for a while
        System.out.println(result);
    }

    private static class PullToRouterActor extends Thread {
        private ZMQ.Socket router;
        private ZMQ.Socket pull;
        private List<String> knownWords;

        PullToRouterActor(ZContext ctx, String routerAddress, String pullAddress, List<String> knownWords) {
            router = ctx.createSocket(ZMQ.ROUTER);
            pull = ctx.createSocket(ZMQ.PULL);
            this.knownWords = knownWords;

            // Connecting the interfaces
            router.bind(routerAddress);
            pull.connect(pullAddress);
        }

        @Override
        public void run() {
            int numberOfDealer = 0;

            // Mining the identities of the dealers
            while (numberOfDealer < knownWords.size()) {
                String msg = router.recvStr(Charset.defaultCharset());
                if (!msg.isEmpty()) {
                    router.recv();
                    numberOfDealer++;
                }
            }

            // Now starts the real deal, where the lines get tokenized and sent to the correct dealer
            // having the tokens as the identity.
            while (!Thread.interrupted()) {

                // Logic to support full and partial match
                List<String> tokens = Arrays.stream(pull.recvStr(Charset.defaultCharset()).split(" "))
                                            .map(word -> {
                                                for (String knownword: knownWords) {
                                                    if (word.toLowerCase().contains(knownword.toLowerCase())) {
                                                        return knownword;
                                                    }
                                                }
                                                return word;
                                            })
                                            .collect(Collectors.toList());

                // Routing words to dealers, or reducers if present.
                for (String token: tokens) {
                    router.sendMore(token);
                    router.sendMore("");
                    router.send(token);
                }
            }
        }
    }

    private static class DealerActor extends Thread {
        private ZMQ.Socket dealer;
        private String word;
        private Map<String, AtomicInteger> result;
        private Integer numberOfDealerAddresses = 0;

        DealerActor(ZContext ctx, List<String> dealerAddresses, String word, Map<String, AtomicInteger> result) {
            dealer = ctx.createSocket(ZMQ.DEALER);
            this.word = word;
            this.result = result;

            // Setting the id of the dealer as the token.
            dealer.setIdentity(word.getBytes());
            for (String dealerAddress: dealerAddresses) {
                dealer.connect(dealerAddress);
                numberOfDealerAddresses++;
            }
        }

        @Override
        public void run() {
            for (int i = 0; i < numberOfDealerAddresses; i++) {
                dealer.send("Register me!");
            }

            while (!Thread.interrupted()) {
                String msg = dealer.recvStr(Charset.defaultCharset());
                if (!msg.isEmpty()) {
                    result.get(word).incrementAndGet();
                }
            }
        }
    }

}