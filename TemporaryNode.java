// IN2011 Computer Networks
// Coursework 2023/2024
//
// Submission by
// YOUR_NAME_GOES_HERE
// YOUR_STUDENT_ID_NUMBER_GOES_HERE
// YOUR_EMAIL_GOES_HERE


import java.io.*;
import java.math.BigInteger;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.List;

// DO NOT EDIT starts
interface TemporaryNodeInterface {
    public boolean start(String startingNodeName, String startingNodeAddress);
    public boolean store(String key, String value);
    public String get(String key);
}
// DO NOT EDIT ends


public class TemporaryNode implements TemporaryNodeInterface {

    public String startingNodeAddres;
    public String startingNodeNam;


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

    // Method to calculate the distance between two hashIDs as per 2D#4
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
                    // Specific handling for END message
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

            // Check if we have received any nodes
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




    public boolean start(String startingNodeName, String startingNodeAddress) {
        this.startingNodeAddres = startingNodeAddress;
        this.startingNodeNam = startingNodeName;

        // Splitting address into host and port for socket creation
        String[] addressComponents = startingNodeAddress.split(":");
        if (addressComponents.length != 2) {
            System.err.println("Invalid starting node address format.");
            return false;
        }
        String host = addressComponents[0];
        int port = Integer.parseInt(addressComponents[1]);

        try (Socket socket = new Socket(host, port);
             OutputStreamWriter outWriter = new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            // Send START message including the highest protocol version supported and the temporary node's name
            outWriter.write("START 1 zain.kashif@city.ac.uk:idk-1\n");
            outWriter.flush();

            // Await and validate the acknowledgment from the starting node
            String response = in.readLine();
            if (response == null || !response.startsWith("START")) {
                System.err.println("Failed to receive valid START acknowledgment.");
                return false;
            }

            // Optionally, use findClosestFullNode method to find and communicate with the closest node
            // For initial start, this might not be necessary unless you're performing operations that require the closest node immediately
            // String closestNodeAddress = findClosestFullNode(someHashID, startingNodeAddress);
            // if (closestNodeAddress == null) {
            //     System.err.println("Failed to find the closest node.");
            //     return false;
            // }

            // At this point, the temporary node is considered successfully started
            // Further operations can be performed as needed
            return true;
        } catch (IOException e) {
            System.err.println("An error occurred while trying to start communication with the network: " + e.getMessage());
            return false;
        }
    }

    public boolean store(String key, String value) {
        try {
            // Ensure the key ends with a newline for consistent hashID computation
            byte[] keyHashBytes = HashID.computeHashID(key);
            String keyHashID = bytesToHex(keyHashBytes);

            // Use the stored startingNodeAddress to find the closest nodes for the key's hashID
            List<String> closestNodeAddresses = findClosestFullNode(keyHashID, startingNodeAddres, startingNodeNam);
            if (closestNodeAddresses.isEmpty()) {
                System.err.println("Failed to find the closest nodes.");
                return false;
            }

            // Attempt to store the (key, value) pair in each of the closest nodes until successful
            for (String nodeAddress : closestNodeAddresses) {
                if (attemptStoreOnNode(key, value, nodeAddress)) {
                    return true; // Storage was successful on this node
                }
                // If storage was not successful, continue to the next node
            }

            // If we get here, storage failed on all nodes
            System.err.println("Failed to store the (key, value) pair on any node.");
            return false;
        } catch (Exception e) {
            System.err.println("An error occurred during the store operation: " + e.getMessage());
            e.printStackTrace();
            return false; // Return false in case of any failure
        }
    }

    private boolean attemptStoreOnNode(String key, String value, String nodeAddress) {
        // Parse the node address into host and port
        String[] parts = nodeAddress.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        // Try to open a socket to the node
        try (Socket socket = new Socket(host, port);
             OutputStreamWriter outWriter = new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            // Send a START message
            outWriter.write("START 1 zain.kashif@city.ac.uk:idk-1\n");
            outWriter.flush();
            String startResponse = in.readLine();


            // Send a PUT? request
            outWriter.write("PUT? 1 1\n" + key + value + "\n");
            outWriter.flush();

            // Read the response
            String response = in.readLine();
            if ("SUCCESS".equals(response)) {
                // Send an END message
                outWriter.write("END Successful_storage\n");
                outWriter.flush();
                return true;  // Storage was successful on this node
            } else if ("FAILED".equals(response)) {
                // Send an END message
                outWriter.write("END Storage_failed\n");
                outWriter.flush();
                return false;  // Node refused to store the value
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return false;  // In case of error or refusal to store
    }

    private String queryNodeForValue(String key, String nodeAddress) {
        // Parse the node address into host and port
        String[] parts = nodeAddress.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        // Try to open a socket to the node
        try (Socket socket = new Socket(host, port);
             OutputStreamWriter outWriter = new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            // Send a START message
            outWriter.write("START 1 zain.kashif@city.ac.uk:idk-1\n");
            outWriter.flush();
            in.readLine();

            // Send a GET? request
            int numberOfLines = key.split("\\n").length; // Subtract 1 because the last split element is after the final newline

            // Send a GET? request with the correct number of lines
            outWriter.write(String.format("GET? %d\n%s", numberOfLines, key));
            outWriter.flush();

            // Read the response
            String response = in.readLine();
            System.out.println("GET? response is: " + response);
            if (response != null && response.startsWith("VALUE")) {
                int count = Integer.parseInt(response.split(" ")[1]);
                StringBuilder value = new StringBuilder();
                for (int i = 0; i < count; i++) {
                    value.append(in.readLine());
                    if (i < count - 1) {
                        value.append("\n");
                    }
                }
                System.out.println("Value is: "+value);

                // Optional: Send an END message to terminate the connection
                outWriter.write("END Successful_retrieval\n");
                outWriter.flush();


                return value.toString();
            } else if ("NOPE".equals(response)) {
                // Optional: Send an END message to indicate no value was found
                outWriter.write("END Key_not_found\n");
                outWriter.flush();
                return null;  // Key not found at this node
            }
        } catch (IOException e) {
            System.err.println("Error during the queryNodeForValue operation: " + e.getMessage());
            e.printStackTrace();
        }

        return null;  // In case of error or the key was not found
    }


    public String get(String key) {
        try {
            // Compute the hashID of the key
            byte[] keyHashBytes = HashID.computeHashID(key);  // Ensuring key ends with newline character
            String keyHashID = bytesToHex(keyHashBytes);

            // Find the closest full node(s) based on the key's hashID
            List<String> closestNodeAddresses = findClosestFullNode(keyHashID, startingNodeAddres, startingNodeNam);
            if (closestNodeAddresses.isEmpty()) {
                System.err.println("Failed to find the closest node.");
                return null;
            }

            // Iterate over the nodes and try to find the value
            for (String nodeAddress : closestNodeAddresses) {
                String value = queryNodeForValue(key, nodeAddress);
                if (value != null) {
                    // Value found, return it
                    return value;
                }
                // If value is null, continue to the next node
            }

            // Value not found in any of the closest nodes
            System.err.println("Value not found for the provided key.");
            return null;
        } catch (Exception e) {
            System.err.println("An error occurred during the GET operation: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }
}
