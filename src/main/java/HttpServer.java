import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import java.nio.charset.Charset;

/**
 * Created by daviddecoding on 8/26/16.
 */
public class HttpServer {

    public static void main(String[] args) {
        ZContext ctx = new ZContext();
        ZMQ.Socket stream = ctx.createSocket(11);

        stream.bind("tcp://127.0.0.1:10001");


        while (!Thread.interrupted()) {
            // The identity
            ZFrame idFrame = ZFrame.recvFrame(stream);

            // The message
            String msg = stream.recvStr(Charset.defaultCharset());
            System.out.println("The message: " + msg);

            idFrame.send(stream, ZMQ.SNDMORE);
            stream.send("Hi!\n");
        }
        stream.close();
        ctx.close();
    }
}
