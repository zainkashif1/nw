// IN2011 Computer Networks
// Coursework 2023/2024
//
// Submission by
// Zain Kashif
// 2200010501
// zain.kashif@city.ac.uk

import java.util.concurrent.*;


import java.io.*;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

// DO NOT EDIT starts
interface FullNodeInterface {
    public boolean listen(String ipAddress, int portNumber);
    public void handleIncomingConnections(String startingNodeName, String startingNodeAddress);
}
// DO NOT EDIT ends


public class FullNode implements FullNodeInterface {
    private final Map<String, String> kvStorage = new ConcurrentHashMap<>();
    private final Map<String, String> networkMap = new ConcurrentHashMap<>();
    private int calculateDistance(String hashID1, String hashID2) {
        // Convert hashID strings to BigInteger for bitwise operations
        BigInteger id1 = new BigInteger(hashID1, 16);
        BigInteger id2 = new BigInteger(hashID2, 16);

        // Perform bitwise XOR to find differing bits
        BigInteger xorResult = id1.xor(id2);

        // Count the number of leading zeros in the XOR result
        // BigInteger does not have a built-in method to count leading zeros
        // Convert to binary string and count leading zeros
        String xorBinStr = xorResult.toString(2);
        int distance = 256 - xorBinStr.length(); // Since xorResult is up to 256 bits

        return distance; // This represents the number of leading matching bits, thus the distance
    }
    private static String bytesToHex(byte[] hash) {
        StringBuilder hexString = new StringBuilder(2 * hash.length);
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }
    public List<String> findClosestFullNode(String targetHashID, String startingNodeAddress, String startingNodeName) throws IOException {

        String[] addressComponents = startingNodeAddress.split(":");
        if (addressComponents.length != 2) {
            throw new IllegalArgumentException("Invalid starting node address format.");
        }
        String host = addressComponents[0];
        int port = Integer.parseInt(addressComponents[1]);

        try (Socket socket = new Socket(host, port);
             OutputStreamWriter outWriter = new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            outWriter.write("START 1 zain.kashif@city.ac.uk:idk-1\n");
            outWriter.flush();

            String startResponse = in.readLine();
            if (startResponse == null || !startResponse.startsWith("START")) {
                throw new IOException("Failed to start communication with the starting node.");
            }

            outWriter.write("NEAREST? " + targetHashID + "\n");
            outWriter.flush();

            String nodesResponse = in.readLine();
            if (nodesResponse == null) {
                throw new IOException("No NODES response received.");
            } else if (!nodesResponse.startsWith("NODES")) {
                if (nodesResponse.startsWith("END")) {
                    System.err.println("Received END message from the starting node.");
                } else {
                    throw new IOException("Invalid NODES response: " + nodesResponse);
                }
            }

            int numberOfNodes = Integer.parseInt(nodesResponse.split(" ")[1]);
            if (numberOfNodes <= 0) {
                throw new IOException("NODES response indicates no nodes available.");
            }

            List<String> nodeAddresses = new ArrayList<>();

            // After the NEAREST? request and you receive the NODES response
            for (int i = 0; i < numberOfNodes; i++) {
                String nodeName = in.readLine();
                String nodeAddress = in.readLine();
                if (nodeName == null || nodeAddress == null) {
                    throw new IOException("Node name or address is missing in the NODES response.");
                }
                nodeAddresses.add(nodeAddress);
            }

            if (nodeAddresses.isEmpty()) {
                throw new IOException("No nodes were found in the NODES response.");
            }
            System.out.println(nodeAddresses);
            return nodeAddresses;


        } catch (IOException e) {
            System.err.println("An error occurred while trying to find the closest node: " + e.getMessage());
            return null;
        }
    }



    public boolean listen(String ipAddress, int portNumber) {
        // Implement this!
        // Return true if the node can accept incoming connections
        // Return false otherwise
        try {
            ServerSocket serverSocket = new ServerSocket(portNumber);
            System.out.println("FullNode listening on " + ipAddress + ":" + portNumber);
            startActiveMappingScheduler();
            // Continuously accept new connections
            new Thread(() -> {
                while (!serverSocket.isClosed()) {
                    try {
                        Socket clientSocket = serverSocket.accept();
                        // Handle each connection in a new thread
                        new Thread(() -> handleConnection(clientSocket)).start();
                    } catch (Exception e) {
                        System.err.println("Error accepting connection: " + e.getMessage());
                    }
                }
            }).start();

            return true; // Successfully started listening
        } catch (Exception e) {
            System.err.println("Failed to start server: " + e.getMessage());
            return false; // Failed to start listening
        }

    }
    private void handleConnection(Socket clientSocket) {
        String clientAddress = clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getPort();
        networkMap.put(clientAddress, "");
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             OutputStreamWriter out = new OutputStreamWriter(clientSocket.getOutputStream(), StandardCharsets.UTF_8)) {

            String requestLine = in.readLine(); // Reads the first line of the request

            while (requestLine != null && !requestLine.equals("END")) {
                System.out.println("Received request: " + requestLine);
                // Parse the request line to determine the action
                String[] requestParts = requestLine.split(" ");
                String command = requestParts[0]; // The command, e.g., GET?, PUT?, NOTIFY?

                switch (command) {
                    case "START":
                        out.write("START 1 zain.kashif@city.ac.uk:zains-implementation-1.0,fullNode-20001");
                        out.flush();
                        break;
                    case "GET?":
                        // Assume additional lines follow based on the protocol for GET? request
                        handleGetRequestLogic(in, out, requestParts);
                        break;
                    case "PUT?":
                        // Assume additional lines follow based on the protocol for PUT? request
                        handlePutRequest(in, out, requestParts);
                        break;
                    case "NOTIFY?":
                        // Handle NOTIFY? request which expects node name and address in subsequent lines
                        String nodeName = in.readLine(); // Node name on the next line
                        String nodeAddress = in.readLine(); // Node address on the following line
                        handleNotifyRequest(out, nodeName, nodeAddress);
                        break;
                    case "ECHO?":
                        // Handle ECHO? request directly
                        handleEchoRequest(out);
                        break;
                    case "NEAREST?":
                        // Handle NEAREST? request which expects a hashID
                        String hashID = requestParts[1]; // HashID is the next part of the request
                        handleNearestRequest(out, hashID);
                        break;
                    default:
                        // Handle unknown command
                        out.write("END_unknown_command");
                        out.flush();
                        networkMap.remove(requestParts[0]);
                        break;
                }
                requestLine = in.readLine(); // Prepare for the next command
            }
        } catch (IOException e) {
            System.err.println("Error handling client connection: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.err.println("Error closing client socket: " + e.getMessage());
            }
        }
    }

    private void handleGetRequestLogic(BufferedReader in, OutputStreamWriter out, String[] requestParts) throws IOException {
        // Parse the number of key lines expected
        int keyLines = Integer.parseInt(requestParts[1]);

        // Protocol specifies that the number of key lines must be more than one
        if (keyLines <= 1) {
            out.write("ERROR Invalid number of key lines; must be more than one.\n");
            out.flush();
            return;
        }

        StringBuilder keyBuilder = new StringBuilder();
        try {
            for (int i = 0; i < keyLines; i++) {
                String line = in.readLine();
                if (line == null) {  // Handle end of stream before expected
                    out.write("ERROR Unexpected end of input stream.\n");
                    out.flush();
                    return;
                }
                keyBuilder.append(line);
                if (i < keyLines - 1) {
                    keyBuilder.append("\n");  // Preserve line breaks as part of the key
                }
            }
        } catch (IOException e) {
            out.write("ERROR Error reading key lines: " + e.getMessage() + "\n");
            out.flush();
            return;
        }

        handleGetRequest(out, keyBuilder.toString());
    }


    private void handlePutRequest(BufferedReader in, OutputStreamWriter out, String[] requestParts) throws IOException {
        int keyLines = Integer.parseInt(requestParts[1]);
        int valueLines = Integer.parseInt(requestParts[2]);
        StringBuilder keyBuilder = new StringBuilder();
        StringBuilder valueBuilder = new StringBuilder();

        try{
        for (int i = 0; i < keyLines; i++) {
            keyBuilder.append(in.readLine());
            if (i < keyLines - 1) keyBuilder.append("\n");
        }

        for (int i = 0; i < valueLines; i++) {
            valueBuilder.append(in.readLine());
            if (i < valueLines - 1) valueBuilder.append("\n");
        }

        String key = keyBuilder.toString();
        String value = valueBuilder.toString();
        String keyHashID = bytesToHex(HashID.computeHashID(key + "\n"));

        List<String> closestNodes = findClosestFullNode(keyHashID, "localhost:20001", ",fullNode-20001");
        String currentNodeHashID = bytesToHex(HashID.computeHashID(key + "\n"));

        if (closestNodes.contains(currentNodeHashID)) {
            kvStorage.put(key, value);
            out.write("SUCCESS\n");
        } else {
            out.write("FAILED\n");
        }
        out.flush();
    } catch (Exception e) {
        out.write("FAILED\n");
        out.flush();
    }
    }



    private void handleGetRequest(OutputStreamWriter out, String key) throws IOException {
        String value = kvStorage.get(key);
        if (value != null) {
            // Split the value to count the lines properly, including empty trailing lines
            String[] valueLines = value.split("\n", -1);
            out.write("VALUE " + valueLines.length + "\n");
            for (String line : valueLines) {
                out.write(line + "\n");
            }
        } else {
            out.write("NOPE\n");
        }
        out.flush();
    }


    /* private void handlePutRequest(OutputStreamWriter out, String key, String value) throws IOException {
        kvStorage.put(key, value);
        out.write("SUCCESS");
        out.flush();
    }*/
    private void handleNotifyRequest(OutputStreamWriter out, String nodeName, String nodeAddress) throws IOException{
        // Add or update the node information in the network map
        networkMap.put(nodeName, nodeAddress);
        out.write("NOTIFIED");
        out.flush();
    }

    private void handleEchoRequest(OutputStreamWriter out) throws IOException{
            out.write("OHCE");
            out.flush();
    }

    // Method to handle NEAREST? requests
    private void handleNearestRequest(OutputStreamWriter out, String targetHashID) throws IOException {
        // Priority queue to maintain the closest nodes with a custom comparator that sorts by distance
        PriorityQueue<Map.Entry<String, Integer>> nodeQueue = new PriorityQueue<>(
                (a, b) -> Integer.compare(a.getValue(), b.getValue())
        );

        // Compute distance for each node and add it to the priority queue
        for (Map.Entry<String, String> entry : networkMap.entrySet()) {
            String nodeHashID = entry.getKey();  // Assuming the key is the node's hashID
            int distance = calculateDistance(targetHashID, nodeHashID);
            nodeQueue.add(new AbstractMap.SimpleEntry<>(entry.getValue(), distance));
        }

        // Building the response with up to 3 closest nodes
        StringBuilder response = new StringBuilder("NODES ");
        int nodesReturned = 0;
        while (!nodeQueue.isEmpty() && nodesReturned < 3) {
            Map.Entry<String, Integer> nearestNode = nodeQueue.poll();
            response.append(nearestNode.getKey()).append("\n");
            nodesReturned++;
        }

        // Insert the count of nodes returned at the correct position in the response
        response.insert(6, nodesReturned + " "); // Adjust the position if needed based on actual format

        // Send the response
        out.write(response.toString().trim() + "\n");
        out.flush();
    }

    private void performActiveMapping() {
        Iterator<Map.Entry<String, String>> it = networkMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            String nodeAddress = entry.getValue();
            try (Socket socket = new Socket(nodeAddress.split(":")[0], Integer.parseInt(nodeAddress.split(":")[1]));
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                // Send a NOTIFY? request or a custom ping message
                out.println("NOTIFY?");
                String response = in.readLine();
                if (!"NOTIFIED".equals(response)) {
                    // If no proper response, remove from map
                    it.remove();
                }
            } catch (IOException e) {
                // If error in connecting, remove from map
                it.remove();
            }
        }
    }


    public void startActiveMappingScheduler() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(this::performActiveMapping, 0, 10, TimeUnit.SECONDS);  // Every 10 seconds
    }

    public void handleIncomingConnections(String startingNodeName, String startingNodeAddress) {
	// Implement this!
        String[] addressParts = startingNodeAddress.split(":");
        String ip = addressParts[0];
        int port = Integer.parseInt(addressParts[1]);

        try (Socket socket = new Socket(ip, port);
             OutputStreamWriter outWriter = new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            // Send START message to the known node
            outWriter.write("START 1 " + startingNodeName + "\n");
            outWriter.flush();

            String startResponse = in.readLine();
            if (startResponse != null && startResponse.startsWith("START")) {
                System.out.println("START handshake successful.");
            } else {
                System.err.println("Failed to complete START handshake.");
                return;
            }

            // Notify the known node about this full node's presence
            String notifyMessage = String.format("NOTIFY?\n%s\n%s\n", startingNodeName, startingNodeAddress);
            outWriter.write(notifyMessage);
            outWriter.flush();

            String notifyResponse = in.readLine();
            if ("NOTIFIED".equals(notifyResponse)) {
                System.out.println("Successfully notified the network about this node.");
            } else {
                System.err.println("Failed to notify the network about this node.");
            }

        } catch (IOException e) {
            System.err.println("IOException while trying to connect to the network: " + e.getMessage());
            e.printStackTrace();
        }
    }

}
