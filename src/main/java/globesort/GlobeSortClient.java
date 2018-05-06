package globesort;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import net.sourceforge.argparse4j.*;
import net.sourceforge.argparse4j.inf.*;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.lang.RuntimeException;
import java.lang.Exception;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Timer;
import java.util.Date;

public class GlobeSortClient {

    private final ManagedChannel serverChannel;
    private final GlobeSortGrpc.GlobeSortBlockingStub serverStub;

	private static int MAX_MESSAGE_SIZE = 100 * 1024 * 1024;

    private String serverStr;

    public GlobeSortClient(String ip, int port) {
        this.serverChannel = ManagedChannelBuilder.forAddress(ip, port)
				.maxInboundMessageSize(MAX_MESSAGE_SIZE)
                .usePlaintext(true).build();
        this.serverStub = GlobeSortGrpc.newBlockingStub(serverChannel);

        this.serverStr = ip + ":" + port;
    }

    public void run(Integer[] values, int num_values) throws Exception {
        System.out.println("Pinging " + serverStr + "...");

        long startTime = System.currentTimeMillis();
        long elapsedTime = 0L;
        serverStub.ping(Empty.newBuilder().build());
        elapsedTime = (new Date()).getTime() - startTime;
        System.out.print("Ping successful. The latency in millisecond is ");
        System.out.println(elapsedTime);

        System.out.println("Pinging " + serverStr + "...");

        startTime = System.currentTimeMillis();
        elapsedTime = 0L;
        serverStub.ping(Empty.newBuilder().build());
        elapsedTime = (new Date()).getTime() - startTime;
        System.out.print("Ping successful. The latency in millisecond is ");
        System.out.println(elapsedTime/2);


        System.out.println("Requesting server to sort array...");

        startTime = System.currentTimeMillis();
        IntArray request = IntArray.newBuilder().addAllValues(Arrays.asList(values)).build();

        
        IntArray response = serverStub.sortIntegers(request);
        elapsedTime = (new Date()).getTime() - startTime;
        System.out.print("Response is ready. The wait time in millisecond is ");
        System.out.println(elapsedTime);
        double app_throughput = (double)num_values/elapsedTime*1000;
        System.out.print("Application level throughput: ");
        System.out.println(app_throughput);
        Integer[] output = response.getValuesList().toArray(new Integer[response.getValuesList().size()]);
        int processing_time = output[output.length - 1];
        double network_throughput = (double)num_values/(elapsedTime - processing_time)*2*1000;
        System.out.print("Network level throughput: ");
        System.out.println(network_throughput);
    }

    public void shutdown() throws InterruptedException {
        serverChannel.shutdown().awaitTermination(2, TimeUnit.SECONDS);
    }

    private static Integer[] genValues(int numValues) {
        ArrayList<Integer> vals = new ArrayList<Integer>();
        Random randGen = new Random();
        for(int i : randGen.ints(numValues).toArray()){
            vals.add(i);
        }
        return vals.toArray(new Integer[vals.size()]);
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("GlobeSortClient").build()
                .description("GlobeSort client");
        parser.addArgument("server_ip").type(String.class)
                .help("Server IP address");
        parser.addArgument("server_port").type(Integer.class)
                .help("Server port");
        parser.addArgument("num_values").type(Integer.class)
                .help("Number of values to sort");

        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }
        return res;
    }

    public static void main(String[] args) throws Exception {
        
        Date cur_date = new Date();
        System.out.println(cur_date.toString());
        
        Namespace cmd_args = parseArgs(args);
        if (cmd_args == null) {
            throw new RuntimeException("Argument parsing failed");
        }

        int num_values = cmd_args.getInt("num_values");
        Integer[] values = genValues(cmd_args.getInt("num_values"));
        long startTime = System.currentTimeMillis();
        long elapsedTime = 0L;
        GlobeSortClient client = new GlobeSortClient(cmd_args.getString("server_ip"), cmd_args.getInt("server_port"));
        try {
            client.run(values, num_values);
        } finally {
            client.shutdown();
        }
        elapsedTime = (new Date()).getTime() - startTime;
        System.out.print("overall main() function consumes time in millisecond: ");
        System.out.println(elapsedTime);
    }
}
