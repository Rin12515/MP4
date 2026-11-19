import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Random;

public class Listener {
    // 0.0 for ACK all packets
    // 0.5 for ACK half packets
    private static final double DEFAULT_DROP_PROBABILITY = 0.0;
    private static final int BUFFER_SIZE = 2048;
    private static final int HANDSHAKE_TIMEOUT_MS = 2000;
    private static final int IDLE_TIMEOUT_MS = 20000;

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: java Listener <talkerIP> <talkerPort> <listenerPort> [dropProbability]");
            System.exit(1);
        }

        InetAddress talkerIP = null;
        try {
            talkerIP = InetAddress.getByName(args[0]);
        } catch (UnknownHostException e) {
            System.err.println("Error: Invalid Talker IP address '" + args[0] + "'.");
            System.exit(1);
        }

        int talkerPort = parsePort(args[1], "Talker");
        int listenerPort = parsePort(args[2], "Listener");

        double dropProbability = DEFAULT_DROP_PROBABILITY;
        if (args.length >= 4) {
            try {
                dropProbability = Double.parseDouble(args[3]);
                if (dropProbability < 0.0 || dropProbability >= 1.0) {
                    System.err.println("Error: Drop probability must be in [0.0, 1.0).");
                    System.exit(1);
                }
            } catch (NumberFormatException e) {
                System.err.println("Error: Invalid drop probability '" + args[3] + "'.");
                System.exit(1);
            }
        }

        Random random = new Random();

        try (DatagramSocket socket = new DatagramSocket(listenerPort)) {
            System.out.println("[Listener] Bound to port " + listenerPort
                    + " (ACK drop probability = " + dropProbability + ")");

            // send CONNECT from bound listenerPort until CONNECTED arrives
            byte[] connectBytes = "CONNECT".getBytes(StandardCharsets.UTF_8);
            DatagramPacket connectPacket =
                    new DatagramPacket(connectBytes, connectBytes.length, talkerIP, talkerPort);

            socket.setSoTimeout(HANDSHAKE_TIMEOUT_MS);
            byte[] recvBuf = new byte[BUFFER_SIZE];
            boolean connected = false;

            while (!connected) {
                System.out.println("[Listener] Sending CONNECT to Talker at "
                        + talkerIP.getHostAddress() + ":" + talkerPort + "...");
                socket.send(connectPacket);

                try {
                    DatagramPacket respPacket = new DatagramPacket(recvBuf, recvBuf.length);
                    socket.receive(respPacket);
                    if (!respPacket.getAddress().equals(talkerIP) || respPacket.getPort() != talkerPort) {
                        continue;
                    }
                    String respStr = new String(
                            respPacket.getData(), respPacket.getOffset(), respPacket.getLength(),
                            StandardCharsets.UTF_8);

                    if ("CONNECTED".equals(respStr)) {
                        connected = true;
                        socket.connect(talkerIP, talkerPort);
                        sendReady(socket);
                        System.out.println("[Listener] Connection established with Talker.\n");
                    }
                } catch (SocketTimeoutException e) {
                    System.out.println("[Listener] No response from Talker yet, retrying...");
                }
            }

            // Talker manages data retransmission timeouts; wait indefinitely until all chunks arrive
            socket.setSoTimeout(0);

            int expectedSeq = 0;
            int totalMessages = -1;
            String[] receivedChunks = null;

            // receive DATA|0|5, DATA|1..5|<Base64>, and linger for DONE / duplicate DATA|5
            while (true) {
                DatagramPacket packet = new DatagramPacket(recvBuf, recvBuf.length);
                try {
                    socket.receive(packet);
                } catch (SocketTimeoutException e) {
                    // Occurs only during post-DATA|5 idle linger if all 3 DONE frames were lost
                    System.out.println("[Listener] Idle timeout reached after final ACK; closing.");
                    break;
                }

                String rawMsg = new String(
                        packet.getData(), packet.getOffset(), packet.getLength(),
                        StandardCharsets.UTF_8);

                // reply READY to every CONNECTED
                if ("CONNECTED".equals(rawMsg)) {
                    sendReady(socket);
                    continue;
                }

                // exit once DONE arrives after all messages have been received
                if ("DONE".equals(rawMsg)) {
                    if (totalMessages != -1 && expectedSeq > totalMessages) {
                        System.out.println("[Listener] Received DONE from Talker.");
                        break;
                    }
                    continue;
                }

                // expected data format: DATA|<seq>|<payload>
                String[] parts = rawMsg.split("\\|", 3);
                if (parts.length != 3 || !"DATA".equals(parts[0])) {
                    System.out.println("[Listener] Malformed packet ignored: " + rawMsg);
                    continue;
                }

                int seqNum;
                try {
                    seqNum = Integer.parseInt(parts[1]);
                } catch (NumberFormatException e) {
                    System.out.println("[Listener] Invalid sequence number ignored: " + parts[1]);
                    continue;
                }
                String payload = parts[2];

                // simulate packet/ACK loss on DATA frames
                if (dropProbability > 0.0 && random.nextDouble() < dropProbability) {
                    System.out.println("[Listener] [Simulated Drop] Received DATA|" + seqNum
                            + " -> NOT sending ACK.");
                    continue;
                }

                if (seqNum == expectedSeq) {
                    if (seqNum == 0) {
                        try {
                            totalMessages = Integer.parseInt(payload.trim());
                            if (totalMessages <= 0) {
                                System.err.println("[Listener] Error: Invalid message count in DATA|0: " + totalMessages);
                                System.exit(1);
                            }
                            receivedChunks = new String[totalMessages];
                            System.out.println("[Listener] Received DATA|0 -> Expecting " + totalMessages + " data messages.");
                        } catch (NumberFormatException e) {
                            System.err.println("[Listener] Error: Corrupted message count in DATA|0.");
                            System.exit(1);
                        }
                    } else {
                        String decodedChunk;
                        try {
                            byte[] decodedBytes = Base64.getDecoder().decode(payload);
                            decodedChunk = new String(decodedBytes, StandardCharsets.UTF_8);
                        } catch (IllegalArgumentException e) {
                            System.out.println("[Listener] Corrupted Base64 in DATA|" + seqNum + "; ignored.");
                            continue;
                        }
                        receivedChunks[seqNum - 1] = decodedChunk;
                        System.out.println("[Listener] Received DATA|" + seqNum + " -> \"" + decodedChunk + "\"");
                    }

                    expectedSeq++;
                    sendAck(socket, expectedSeq);

                    // keep socket open after final ACK to handle DONE or re-ACK duplicate DATA|5
                    if (expectedSeq > totalMessages) {
                        socket.setSoTimeout(IDLE_TIMEOUT_MS);
                    }

                } else if (seqNum < expectedSeq) {
                    System.out.println("[Listener] Duplicate DATA|" + seqNum
                            + " received. Re-sending ACK|" + (seqNum + 1));
                    sendAck(socket, seqNum + 1);
                } else {
                    System.out.println("[Listener] Out-of-order DATA|" + seqNum
                            + " (expected " + expectedSeq + "). Ignored.");
                }
            }

            // reconstruct message
            StringBuilder finalMessage = new StringBuilder();
            if (receivedChunks != null) {
                for (String chunk : receivedChunks) {
                    if (chunk != null) {
                        finalMessage.append(chunk);
                    }
                }
            }

            String result = finalMessage.toString();
            int charCount = result.codePointCount(0, result.length());
            System.out.println("\n[Listener] All messages received. Connection closed.");
            System.out.println("[Listener] Reconstructed String (" + charCount + " chars): \"" + result + "\"");

        } catch (SocketException e) {
            System.err.println("[Listener] Socket error: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("[Listener] I/O error: " + e.getMessage());
        }
    }

    private static void sendReady(DatagramSocket socket) throws IOException {
        byte[] readyBytes = "READY".getBytes(StandardCharsets.UTF_8);
        socket.send(new DatagramPacket(readyBytes, readyBytes.length));
        System.out.println("[Listener] Sent READY");
    }

    private static void sendAck(DatagramSocket socket, int nextSeqNum) throws IOException {
        String ackMsg = "ACK|" + nextSeqNum;
        byte[] ackBytes = ackMsg.getBytes(StandardCharsets.UTF_8);
        socket.send(new DatagramPacket(ackBytes, ackBytes.length));
        System.out.println("[Listener] Sent " + ackMsg + " (requesting DATA|" + nextSeqNum + ")");
    }

    private static int parsePort(String portStr, String role) {
        try {
            int port = Integer.parseInt(portStr);
            if (port < 1 || port > 65535) {
                System.err.println("Error: " + role + " port must be between 1 and 65535.");
                System.exit(1);
            }
            return port;
        } catch (NumberFormatException e) {
            System.err.println("Error: Invalid " + role + " port number '" + portStr + "'.");
            System.exit(1);
            return -1;
        }
    }
}