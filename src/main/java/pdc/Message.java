package pdc;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Wire Format (Binary, Length-Prefixed):
 * [4 bytes: total length] [magic] [version] [messageType] [sId] [timestamp] [payload]
 */
public class Message {
    public String magic;
    public int version;
    public String type;           // Internal field
    public String messageType;    // Protocol field (alias for type)
    public String sender;          // Internal field
    public String sId;            // Protocol field (alias for sender)
    public long timestamp;
    public byte[] payload;

    public Message() {
        this.magic = "CSM218";
        this.version = 1;
        this.timestamp = System.currentTimeMillis();
        this.payload = new byte[0];
        // Keep fields in sync
        this.messageType = this.type;
        this.sId = this.sender;
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Uses length-prefixing to handle TCP stream boundaries.
     */
    public byte[] pack() {
        try {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buffer);

            // Write magic string (length-prefixed)
            writeString(out, this.magic != null ? this.magic : "CSM218");
            
            // Write version
            out.writeInt(this.version);
            
            // Write message type (length-prefixed) - use messageType if set, otherwise type
            writeString(out, this.messageType != null ? this.messageType : (this.type != null ? this.type : ""));
            
            // Write sender/sId (length-prefixed) - use sId if set, otherwise sender
            writeString(out, this.sId != null ? this.sId : (this.sender != null ? this.sender : ""));
            
            // Write timestamp
            out.writeLong(this.timestamp);
            
            // Write payload (length-prefixed)
            if (this.payload != null) {
                out.writeInt(this.payload.length);
                out.write(this.payload);
            } else {
                out.writeInt(0);
            }

            out.flush();
            byte[] messageBytes = buffer.toByteArray();
            
            // Prepend total length for framing
            ByteArrayOutputStream framedBuffer = new ByteArrayOutputStream();
            DataOutputStream framedOut = new DataOutputStream(framedBuffer);
            framedOut.writeInt(messageBytes.length);
            framedOut.write(messageBytes);
            framedOut.flush();
            
            return framedBuffer.toByteArray();
            
        } catch (IOException e) {
            throw new RuntimeException("Failed to pack message", e);
        }
    }

    /**
     * Reconstructs a Message from a byte stream.
     * Validates CSM218 protocol compliance.
     */
    public static Message unpack(byte[] data) {
        if (data == null || data.length < 4) {
            throw new IllegalArgumentException("Invalid message data");
        }
        
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
            
            // Read total length (frame header)
            int totalLength = in.readInt();
            
            Message msg = new Message();
            
            // Read magic string
            msg.magic = readString(in);
            
            // Validate protocol
            if (!"CSM218".equals(msg.magic)) {
                throw new IllegalArgumentException("Invalid magic: expected CSM218, got " + msg.magic);
            }
            
            // Read version
            msg.version = in.readInt();
            
            if (msg.version != 1) {
                throw new IllegalArgumentException("Unsupported protocol version: " + msg.version);
            }
            
            // Read message type
            msg.messageType = readString(in);
            msg.type = msg.messageType;  // Keep in sync

            // Read sender/sId
            msg.sId = readString(in);
            msg.sender = msg.sId;  // Keep in sync
            
            // Read timestamp
            msg.timestamp = in.readLong();
            
            // Read payload
            int payloadLength = in.readInt();
            if (payloadLength > 0) {
                msg.payload = new byte[payloadLength];
                in.readFully(msg.payload);
            } else {
                msg.payload = new byte[0];
            }
            
            return msg;
            
        } catch (IOException e) {
            throw new RuntimeException("Failed to unpack message", e);
        }
    }

    /**
     * Pack message into a ByteBuffer for efficient NIO-based transmission.
     * Uses direct ByteBuffer for zero-copy optimization where possible.
     */
    public ByteBuffer packToBuffer() {
        byte[] packed = pack();
        ByteBuffer buffer = ByteBuffer.allocateDirect(packed.length);
        buffer.put(packed);
        buffer.flip();
        return buffer;
    }

    /**
     * Read a complete message from an InputStream, handling TCP fragmentation.
     * Uses a while loop to read all bytes even when the payload spans multiple
     * TCP segments (jumbo frames / payloads larger than MTU).
     */
    public static byte[] readFullyFromStream(InputStream in, int length) throws IOException {
        byte[] data = new byte[length];
        int offset = 0;
        while (offset < length) {
            int bytesRead = in.read(data, offset, length - offset);
            if (bytesRead == -1) {
                throw new EOFException("Stream ended after " + offset + " of " + length + " bytes");
            }
            offset += bytesRead;
        }
        return data;
    }

    /**
     * Read a framed message from a raw InputStream.
     * Handles TCP fragmentation for jumbo payloads (8MB+) by reading
     * in a loop until all bytes are received.
     */
    public static Message readFromStream(InputStream rawIn) throws IOException {
        DataInputStream in = new DataInputStream(rawIn);
        int totalLength = in.readInt();
        if (totalLength < 0 || totalLength > 64_000_000) {
            throw new IOException("Invalid frame length: " + totalLength);
        }
        byte[] frameData = readFullyFromStream(rawIn, totalLength);
        byte[] fullMessage = new byte[totalLength + 4];
        ByteBuffer header = ByteBuffer.allocate(4);
        header.putInt(totalLength);
        System.arraycopy(header.array(), 0, fullMessage, 0, 4);
        System.arraycopy(frameData, 0, fullMessage, 4, totalLength);
        return unpack(fullMessage);
    }

    /**
     * Helper: Write a length-prefixed UTF-8 string
     */
    private static void writeString(DataOutputStream out, String str) throws IOException {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    /**
     * Helper: Read a length-prefixed UTF-8 string
     */
    private static String readString(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > 1_000_000) {
            throw new IOException("Invalid string length: " + length);
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}