import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.PortUnreachableException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * ECEN 424 MP4: Stop-and-wait reliable transfer over UDP.
 *
 * Usage: java Talker <talkerPort> <listenerPort> "<message up to 50 characters>"
 *
 * Protocol (all frames are UTF-8 text, no trailing newline):
 *   Listener -> Talker  : CONNECT                 (sent from the Listener's bound listenerPort)
 *   Talker   -> Listener: CONNECTED               (retransmitted every 2 s until READY)
 *   Listener -> Talker  : READY                   (reply to EVERY CONNECTED, in any state; never dropped)
 *   Talker   -> Listener: DATA|0|5                (message 0 = number of data messages)
 *   Listener -> Talker  : ACK|1                   (ACK = next sequence number expected)
 *   Talker   -> Listener: DATA|<n>|<Base64 UTF-8> (n = 1..5, payload may be empty)
 *   Listener -> Talker  : ACK|<n+1>
 *   Talker   -> Listener: DONE                    (best-effort, sent 3 times, never ACKed)
 *
 * Exactly five data frames are always sent (empty chunks for short input).
 * The Listener must keep re-ACKing duplicate DATA|5 until DONE or an idle timeout,
 * so a randomly suppressed ACK|6 cannot strand the transfer.
 */
public final class Talker {
    private static final int CHUNK_COUNT = 5;
    private static final int CHUNK_SIZE = 10;
    private static final int MAX_CHARACTERS = CHUNK_COUNT * CHUNK_SIZE;
    private static final int ACK_TIMEOUT_MS = 2000;
    private static final int MAX_ATTEMPTS = 30;
    private static final int MAX_PACKET_BYTES = 2048;

    private Talker() { }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: java Talker <talkerPort> <listenerPort> \"<message>\"");
            System.exit(1);
        }

        try {
            int talkerPort = parsePort(args[0], "Talker");
            int listenerPort = parsePort(args[1], "Listener");
            String message = args[2];
            int characters = message.codePointCount(0, message.length());

            if (characters > MAX_CHARACTERS) {
                throw new IllegalArgumentException(
                    "Message contains " + characters + " characters; maximum is " + MAX_CHARACTERS);
            }

            try (DatagramSocket socket = new DatagramSocket(talkerPort)) {
                System.out.println("[Talker] Waiting for CONNECT on UDP port " + talkerPort + "...");
                InetAddress listenerAddress = waitForConnect(socket, listenerPort);
                System.out.println("[Talker] CONNECT received from "
                    + listenerAddress.getHostAddress() + ":" + listenerPort);

                // Filters incoming datagrams to the Listener's IP:port.
                socket.connect(listenerAddress, listenerPort);
                sendAndWaitFor(socket, "CONNECTED", "READY", "Handshake");
                System.out.println("[Talker] Listener ready on "
                    + listenerAddress.getHostAddress() + ":" + listenerPort);

                sendAndWaitFor(socket, "DATA|0|" + CHUNK_COUNT, "ACK|1", "Message 0");

                for (int i = 0; i < CHUNK_COUNT; i++) {
                    String chunk = extractChunk(message, characters, i);
                    String encoded = Base64.getEncoder().encodeToString(
                        chunk.getBytes(StandardCharsets.UTF_8));
                    int sequence = i + 1;
                    String frame = "DATA|" + sequence + "|" + encoded;
                    sendAndWaitFor(socket, frame, "ACK|" + (sequence + 1),
                        "Message " + sequence + " (" + chunk.codePointCount(0, chunk.length())
                            + " chars: \"" + chunk + "\")");
                }

                // The transfer is already fully acknowledged. DONE is only a courtesy, so a
                // failure here (e.g. ICMP port-unreachable after the Listener closed) is ignored.
                for (int i = 0; i < 3; i++) {
                    try {
                        send(socket, "DONE");
                    } catch (IOException ignored) {
                        // best-effort notification
                    }
                }
                System.out.println("[Talker] All five messages acknowledged; transfer complete.");
            }
        } catch (IllegalArgumentException e) {
            System.err.println("Input error: " + e.getMessage());
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Network error: " + e.getMessage());
            System.exit(1);
        }
    }

    private static int parsePort(String value, String name) {
        final int port;
        try {
            port = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " port must be an integer (1-65535).");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException(name + " port must be between 1 and 65535.");
        }
        return port;
    }

    /** Waits for CONNECT; only accepts one whose source port equals the configured listenerPort. */
    private static InetAddress waitForConnect(DatagramSocket socket, int listenerPort)
            throws IOException {
        socket.setSoTimeout(0); // wait as long as needed
        Set<Integer> warnedPorts = new HashSet<>();
        while (true) {
            DatagramPacket packet = receive(socket);
            String text = readText(packet);
            if (!"CONNECT".equals(text)) {
                System.out.println("[Talker] Ignored unexpected connection request: " + text);
                continue;
            }
            if (packet.getPort() != listenerPort) {
                if (warnedPorts.add(packet.getPort())) { // warn once per distinct port
                    System.err.println("[Talker] Ignoring CONNECT from port " + packet.getPort()
                        + "; the Listener must send from its bound port " + listenerPort);
                }
                continue;
            }
            return packet.getAddress();
        }
    }

    private static String extractChunk(String input, int charCount, int chunkIndex) {
        int start = Math.min(chunkIndex * CHUNK_SIZE, charCount);
        int end = Math.min((chunkIndex + 1) * CHUNK_SIZE, charCount);
        int startOffset = input.offsetByCodePoints(0, start);
        int endOffset = input.offsetByCodePoints(0, end);
        return input.substring(startOffset, endOffset);
    }

    /** Send, then retransmit every 2 s until the exact expected response arrives. */
    private static void sendAndWaitFor(
            DatagramSocket socket, String frame, String expectedResponse, String description)
            throws IOException {
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            System.out.println("[Talker] SEND " + description + " [attempt " + attempt
                + "/" + MAX_ATTEMPTS + "]: " + frame);

            // One 2 s deadline per attempt, regardless of stray packets.
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ACK_TIMEOUT_MS);
            try {
                send(socket, frame);
                while (true) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) {
                        break;
                    }
                    socket.setSoTimeout(
                        (int) Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
                    try {
                        String response = readText(receive(socket));
                        if (expectedResponse.equals(response)) {
                            System.out.println("[Talker] RECV " + response + " -> accepted");
                            return;
                        }
                        System.out.println("[Talker] RECV " + response + " -> ignored"
                            + " (expected " + expectedResponse + ")");
                    } catch (SocketTimeoutException e) {
                        break;
                    }
                }
            } catch (PortUnreachableException e) {
                // ICMP is advisory (and may be filtered), so treat it as one lost attempt and let
                // MAX_ATTEMPTS decide. Sleep to the deadline so the loop cannot spin.
                System.out.println("[Talker] ICMP port unreachable; counting as a lost attempt");
                sleepUntil(deadline);
            }
            if (attempt < MAX_ATTEMPTS) {
                System.out.println("[Talker] TIMEOUT after 2 seconds; retransmitting " + description);
            }
        }
        throw new IOException("No " + expectedResponse + " after " + MAX_ATTEMPTS
            + " attempts for " + description + ".");
    }

    private static void sleepUntil(long deadlineNanos) {
        long ms = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
        if (ms > 0) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static DatagramPacket receive(DatagramSocket socket) throws IOException {
        byte[] bytes = new byte[MAX_PACKET_BYTES]; // fresh buffer per receive
        DatagramPacket packet = new DatagramPacket(bytes, bytes.length);
        socket.receive(packet);
        return packet;
    }

    private static String readText(DatagramPacket packet) {
        return new String(packet.getData(), packet.getOffset(), packet.getLength(),
            StandardCharsets.UTF_8);
    }

    private static void send(DatagramSocket socket, String message) throws IOException {
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        socket.send(new DatagramPacket(bytes, bytes.length)); // socket is connected to the Listener
    }
}
