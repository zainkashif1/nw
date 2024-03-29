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
import java.security.NoSuchAlgorithmException;

// DO NOT EDIT starts
interface TemporaryNodeInterface {
    public boolean start(String startingNodeName, String startingNodeAddress);
    public boolean store(String key, String value);
    public String get(String key);
}
// DO NOT EDIT ends


public class TemporaryNode implements TemporaryNodeInterface {

    public String startingNodeAddres;

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

    private String computeHashIDString(String input) throws NoSuchAlgorithmException {
        byte[] hash = new byte[0]; // Appending newline to comply with your HashID requirements
        try {
            hash = HashID.computeHashID(input + "\n");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return bytesToHex(hash);
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


    public String findClosestFullNode(String targetHashID, String startingNodeAddress) throws IOException {
        String[] addressComponents = startingNodeAddress.split(":");
        if (addressComponents.length != 2) {
            throw new IllegalArgumentException("Invalid starting node address format.");
        }
        String host = addressComponents[0];
        int port = Integer.parseInt(addressComponents[1]);

        try (Socket socket = new Socket(host, port);
             OutputStreamWriter outWriter = new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            outWriter.write("START 1 TemporaryNode\n");
            outWriter.flush();

            // Enhanced logging for debugging
            System.out.println("Sent START message to the node.");

            String startResponse = in.readLine();
            if (startResponse == null) {
                throw new IOException("No response after START message.");
            } else if (!startResponse.startsWith("START")) {
                throw new IOException("Invalid response after START message: " + startResponse);
            }

            // Enhanced logging for debugging
            System.out.println("Received START acknowledgment: " + startResponse);

            outWriter.write("NEAREST? " + targetHashID + "\n");
            outWriter.flush();

            // Enhanced logging for debugging
            System.out.println("Sent NEAREST? request with hashID: " + targetHashID);

            String nodesResponse = in.readLine();
            System.out.println("Received NODES response: " + nodesResponse);
            if (nodesResponse == null) {
                throw new IOException("No NODES response received.");
            } else if (!nodesResponse.startsWith("NODES")) {
                if (nodesResponse.startsWith("END")) {
                    throw new IOException("Received END message before finding a node: " + nodesResponse);
                } else {
                    throw new IOException("Invalid NODES response: " + nodesResponse);
                }
            }

            // Enhanced logging for debugging
            System.out.println("Received NODES response: " + nodesResponse);

            int numberOfNodes = Integer.parseInt(nodesResponse.split(" ")[1]);
            if (numberOfNodes <= 0) {
                throw new IOException("NODES response indicates no nodes available.");
            }

            for (int i = 0; i < numberOfNodes; i++) {
                String nodeName = in.readLine();
                String nodeAddress = in.readLine();

                if (nodeName == null || nodeAddress == null) {
                    throw new IOException("Node name or address is missing in the NODES response.");
                }

                // Enhanced logging for debugging
                System.out.println("Node name: " + nodeName);
                System.out.println("Node address: " + nodeAddress);

                return nodeAddress; // Returns the address of the first closest node for simplicity
            }

            throw new IOException("Failed to parse NODES response correctly.");
        } catch (NumberFormatException ex) {
            throw new IOException("Port number format error: " + startingNodeAddress, ex);
        }
    }




    public boolean start(String startingNodeName, String startingNodeAddress) {
        // Implement this!
        // Return true if the 2D#4 network can be contacted
        // Return false if the 2D#4 network can't be contacted
        startingNodeAddres = startingNodeAddress;
        try {
            // First, establish a basic connection with the starting node and perform initial communication
            String nodeName = startingNodeName;
            try (Socket socket = new Socket(startingNodeAddress.split(":")[0], Integer.parseInt(startingNodeAddress.split(":")[1]));
                 PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                // Send START message including the highest protocol version supported and the temporary node's name
                out.println("START 1 " + nodeName+"\n");
                out.flush();

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
            }
        } catch (Exception e) {
            System.err.println("An error occurred while trying to start communication with the network: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    public boolean store(String key, String value) {
        // Implement this!
        // Return true if the store worked
        // Return false if the store failed
        try {
            // Ensure the key ends with a newline for consistent hashID computation
            byte[] keyHashBytes = HashID.computeHashID(key+"\n");
            String keyHashID = bytesToHex(keyHashBytes);

            // Use the stored startingNodeAddress to find the closest node for the key's hashID
            String closestNodeAddress = findClosestFullNode(keyHashID, startingNodeAddres);
            if (closestNodeAddress == null) {
                System.err.println("Failed to find the closest node.");
                return false;
            }

            // Split the closest node address into IP and port
            String[] addressParts = closestNodeAddress.split(":");
            if (addressParts.length != 2) {
                System.err.println("Invalid node address format.");
                return false;
            }

            try (Socket socket = new Socket(addressParts[0], Integer.parseInt(addressParts[1]));
                 PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                // Send START message to the closest node
                out.println("START 1 TemporaryNode\n"); // Here "TemporaryNode" could be more descriptive if necessary
                out.flush();

                // Send the PUT? request with the key and value
                out.println("PUT? 1 1"); // Assuming the key and value are each considered one line
                out.println(key + "\n");
                out.println(value+"\n");
                out.flush();

                // Read and process the response from the closest node
                String responseHeader = in.readLine();
                if ("SUCCESS".equals(responseHeader)) {
                    return true; // Storage was successful
                } else if ("FAILED".equals(responseHeader)) {
                    return false; // Storage failed
                }

                // Always send an END message to cleanly terminate the protocol interaction
                out.println("END Storage attempt completed");
                out.flush();
            }
        } catch (Exception e) {
            System.err.println("An error occurred during the store operation: " + e.getMessage());
            e.printStackTrace();
        }
        return false; // Return false in case of any failure
    }

    public String get(String key) {
        try {
            // First, compute the hashID of the key.
            byte[] keyHashBytes = HashID.computeHashID(key); // Ensure the key ends with a newline character.
            String keyHashID = bytesToHex(keyHashBytes);

            // Then, find the closest node based on the key's hashID. This step may vary depending on how you implement findClosestFullNode.
            String closestNodeAddress = findClosestFullNode(keyHashID, startingNodeAddres);
            if (closestNodeAddress == null) {
                System.err.println("Failed to find the closest node.");
                return null;
            }

            // Split the closest node address into IP and port.
            String[] addressParts = closestNodeAddress.split(":");
            if (addressParts.length != 2) {
                System.err.println("Invalid node address format.");
                return null;
            }

            try (Socket socket = new Socket(addressParts[0], Integer.parseInt(addressParts[1]));
                 PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                // Send START message to the closest node.
                out.println("START 1 TemporaryNode\n"); // Here, "TemporaryNode" could be replaced with a more descriptive name if necessary.
                out.flush();

                // Send the GET? request for the specified key.
                int linesInKey = key.contains("\n") ? key.split("\n").length : 1; // Adjust based on how you count lines in key
                out.println("GET? " + linesInKey+ "\n");
                out.println(key + "\n"); // Ensure the key ends with a newline.
                out.flush();

                // Read and process the response from the closest node.
                String responseHeader = in.readLine();
                if (responseHeader != null && responseHeader.startsWith("VALUE")) {
                    int numberOfValueLines = Integer.parseInt(responseHeader.split(" ")[1]);
                    StringBuilder valueBuilder = new StringBuilder();
                    for (int i = 0; i < numberOfValueLines; i++) {
                        valueBuilder.append(in.readLine());
                        if (i < numberOfValueLines - 1) {
                            valueBuilder.append("\n");
                        }
                    }
                    return valueBuilder.toString();
                } else if ("NOPE".equals(responseHeader)) {
                    return null; // Key not found in the network.
                }

                // Always send an END message to cleanly terminate the protocol interaction.
                out.println("END Successful retrieval\n");
                out.flush();
            }
        } catch (Exception e) {
            System.err.println("An error occurred during the GET operation: " + e.getMessage());
            e.printStackTrace();
        }
        return null; // Return null in case of any failure.
    }
}
