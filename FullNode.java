// IN2011 Computer Networks
// Coursework 2023/2024
//
// Submission by
// Zain Kashif
// 2200010501
// zain.kashif@city.ac.uk


import java.io.*;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
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



    public boolean listen(String ipAddress, int portNumber) {
        // Implement this!
        // Return true if the node can accept incoming connections
        // Return false otherwise
        try {
            ServerSocket serverSocket = new ServerSocket(portNumber);
            System.out.println("FullNode listening on " + ipAddress + ":" + portNumber);

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
                        out.write("START 1 dontknow");
                        out.flush();
                        break;
                    case "GET?":
                        // Assume additional lines follow based on the protocol for GET? request
                        handleGetRequestLogic(in, out, requestParts);
                        break;
                    case "PUT?":
                        // Assume additional lines follow based on the protocol for PUT? request
                        handlePutRequestLogic(in, out, requestParts);
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
        int keyLines = Integer.parseInt(requestParts[1]);
        StringBuilder keyBuilder = new StringBuilder();
        for (int i = 0; i < keyLines; i++) {
            keyBuilder.append(in.readLine());
            if (i < keyLines - 1) keyBuilder.append("\n");
        }
        handleGetRequest(out, keyBuilder.toString());
    }

    private void handlePutRequestLogic(BufferedReader in, OutputStreamWriter out, String[] requestParts) throws IOException {
        int keyLines = Integer.parseInt(requestParts[1]);
        int valueLines = Integer.parseInt(requestParts[2]);
        StringBuilder keyBuilder = new StringBuilder();
        StringBuilder valueBuilder = new StringBuilder();
        for (int i = 0; i < keyLines; i++) {
            keyBuilder.append(in.readLine());
        }
        for (int i = 0; i < valueLines; i++) {
            valueBuilder.append(in.readLine());
        }
        handlePutRequest(out, keyBuilder.toString(), valueBuilder.toString());
    }

    private void handleGetRequest(OutputStreamWriter out, String key) throws IOException {
        String value = kvStorage.get(key);
        if (value != null) {
            // Split the value by new lines to accurately report the number of lines
            String[] valueLines = value.split("\n", -1);  // The -1 limit parameter makes it include empty trailing strings
            out.write("VALUE " + valueLines.length + "\n");
            for (String line : valueLines) {
                out.write(line + "\n");
            }
        } else {
            out.write("NOPE\n");
        }
        out.flush();  // Ensure to flush after handling both cases
    }

    private void handlePutRequest(OutputStreamWriter out, String key, String value) throws IOException {
        kvStorage.put(key, value);
        out.write("SUCCESS");
        out.flush();
    }
    private void handleNotifyRequest(OutputStreamWriter out, String nodeName, String nodeAddress) throws IOException{
        // Add or update the node information in the network map
        networkMap.put(nodeName, nodeAddress);
        out.write("NOTIFIED");
        out.flush();
    }

    // Method to handle ECHO? requests
    private void handleEchoRequest(OutputStreamWriter out) throws IOException{
            out.write("OHCE");
            out.flush();
    }

    // Method to handle NEAREST? requests
    private void handleNearestRequest(OutputStreamWriter out, String hashID) throws IOException {
        // Simplified example: Return up to 3 closest nodes from the network map
        // In a real implementation, you'd calculate distances based on hashIDs
        // For simplicity, we'll just return the first three nodes (or fewer, if not enough nodes are known)
        int nodesReturned = 0;
        StringBuilder response = new StringBuilder("NODES ");
        for (Map.Entry<String, String> entry : networkMap.entrySet()) {
            if (nodesReturned >= 3) break; // Limit to 3 nodes
            response.append(entry.getKey()).append(" ").append(entry.getValue()).append("\n");
            nodesReturned++;
        }
        response.insert(6, nodesReturned); // Insert the count of nodes at the correct position
        out.write(response.toString().trim()); // Remove the last newline
        out.flush();
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
