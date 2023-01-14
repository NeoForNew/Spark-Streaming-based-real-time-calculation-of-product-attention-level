package com.shiyanlou.simulator;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
public class SimulatorSocket {
    public static void main(String[] args) throws Exception {
        // Create a thread to start the simulator
        new Thread(new SimulatorSocketLog()).start();
    }
}
class SimulatorSocketLog implements Runnable{
    // Suppose there are 200 items in total
    private int GOODSID = 200;
    // Number of random messages sent
    private int MSG_NUM = 30;
    // Assume that the number of times the user views the item
    private int BROWSE_NUM = 5;
    // Assume that the user stays for the product browsing time
    private int STAY_TIME = 10;
    // Used to reflect whether the user favorite, favorite is 1, not favorite is 0, poor rating -1
    int[] COLLECTION = new int[]{-1,0,1};
    // Used to simulate the number of pieces of goods purchased by the user, 0 is more to increase the
    // probability of not buying, after all, do not buy or a lot of users are just looking at
    private int[] BUY_NUM = new int[]{0,1,0,2,0,0,0,1,0};
    public void run() {
        // TODO Auto-generated method stub
        Random r = new Random();

        try {
            /**
             *create a server side, listening to port 9999, the client is Streaming,
             * by looking at the source code to know that Streaming *socketTextStream
             * is actually equivalent to a client
             */
            ServerSocket sScoket = new ServerSocket(9999);
            System.out.println("Successfully turn on the data simulation module and go run the Streaming program!");
            while(true){
                // Number of random messages
                int msgNum = r.nextInt(MSG_NUM)+1;
                // Start Listening
                Socket socket = sScoket.accept();
                // Creating output streams
                OutputStream os = socket.getOutputStream();
                // Package output stream
                PrintWriter pw = new PrintWriter(os);
                for (int i = 0; i < msgNum; i++) {
                    // Message format: Product ID::Views::Length of stay::Whether to collect::Number of pieces purchased
                    StringBuffer sb = new StringBuffer();
                    sb.append("goodsID-"+(r.nextInt(GOODSID)+1));
                    sb.append("::");
                    sb.append(r.nextInt(BROWSE_NUM)+1);
                    sb.append("::");
                    sb.append(r.nextInt(STAY_TIME)+r.nextFloat());
                    sb.append("::");
                    sb.append(COLLECTION[r.nextInt(2)]);
                    sb.append("::");
                    sb.append(BUY_NUM[r.nextInt(9)]);
                    System.out.println(sb.toString());
                    //Send a message
                    pw.write(sb.toString()+"\n");
                }
                pw.flush();
                pw.close();
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    System.out.println("thread sleep failed");
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            System.out.println("port used");
        }

    }

}