// IN2011 Computer Networks
// Coursework 2023/2024
//
// Submission by
// YOUR_NAME_GOES_HERE
// YOUR_STUDENT_ID_NUMBER_GOES_HERE
// YOUR_EMAIL_GOES_HERE


import java.io.*;
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
             PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

            String requestLine = in.readLine(); // Reads the first line of the request

            while (requestLine != null && !requestLine.equals("END")) {
                System.out.println("Received request: " + requestLine);
                // Parse the request line to determine the action
                String[] requestParts = requestLine.split(" ");
                String command = requestParts[0]; // The command, e.g., GET?, PUT?, NOTIFY?

                switch (command) {
                    case "START":
                        out.println("START 1 dontknow");
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
                        out.println("ERROR Unknown command");
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

    private void handleGetRequestLogic(BufferedReader in, PrintWriter out, String[] requestParts) throws IOException {
        int keyLines = Integer.parseInt(requestParts[1]);
        StringBuilder keyBuilder = new StringBuilder();
        for (int i = 0; i < keyLines; i++) {
            keyBuilder.append(in.readLine());
            if (i < keyLines - 1) keyBuilder.append("\n");
        }
        handleGetRequest(out, keyBuilder.toString());
    }

    private void handlePutRequestLogic(BufferedReader in, PrintWriter out, String[] requestParts) throws IOException {
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

    private void handleGetRequest(PrintWriter out, String key) {
        String value = kvStorage.get(key);
        if (value != null) {
            out.println("VALUE 1");
            out.println(value);
        } else {
            out.println("NOPE");
        }
        out.flush();
    }

    private void handlePutRequest(PrintWriter out, String key, String value) {
        kvStorage.put(key, value);
        out.println("SUCCESS");
        out.flush();
    }
    private void handleNotifyRequest(PrintWriter out, String nodeName, String nodeAddress) {
        // Add or update the node information in the network map
        networkMap.put(nodeName, nodeAddress);
        out.println("NOTIFIED");
        out.flush();
    }

    // Method to handle ECHO? requests
    private void handleEchoRequest(PrintWriter out) {
        out.println("OHCE");
        out.flush();
    }

    // Method to handle NEAREST? requests
    private void handleNearestRequest(PrintWriter out, String hashID) {
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
        out.println(response.toString().trim()); // Remove the last newline
        out.flush();
    }

    public void handleIncomingConnections(String startingNodeName, String startingNodeAddress) {
	// Implement this!
        String[] addressParts = startingNodeAddress.split(":");
        String ip = addressParts[0];
        int port = Integer.parseInt(addressParts[1]);

        try (Socket socket = new Socket(ip, port);
             OutputStream os = socket.getOutputStream();
             PrintWriter out = new PrintWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            // Send START message to the known node
            out.println("START 1 " + startingNodeName);
            String startResponse = in.readLine();
            if (startResponse != null && startResponse.startsWith("START")) {
                System.out.println("START handshake successful.");
            } else {
                System.err.println("Failed to complete START handshake.");
                return;
            }

            // Notify the known node about this full node's presence
            // You might need to replace "127.0.0.1" with your actual public IP or a reachable hostname
            // and "this.listeningPort" with the port number your full node listens on for incoming connections
            String notifyMessage = String.format("NOTIFY?\n%s\n%s:%d\n", startingNodeName, "10.0.0.151", 20000);
            out.println(notifyMessage);
            out.flush();

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
