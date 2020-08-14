package ch.uzh.ifi.swhdiff;

import java.io.IOException;
import java.math.BigInteger;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.SwhPID;
import org.softwareheritage.graph.backend.MapBuilder;
import org.softwareheritage.graph.backend.MapFile;

/**
 * Mapping between internal long node id and external SWH PID.
 *
 * Mappings in both directions are pre-computed and dumped on disk in the
 * {@link MapBuilder} class, then they are loaded here using mmap().
 *
 * @author The Software Heritage developers
 * @see org.softwareheritage.graph.backend.MapBuilder
 */

public class NodeIdMap {
    /** Graph path and basename */
    String graphPath;
    /** Number of ids to map */
    long nbIds;
    /** mmap()-ed PID_TO_NODE file */
    MapFile swhToNodeMap;
    /** mmap()-ed NODE_TO_PID file */
    MapFile nodeToSwhMap;

    /**
     * Constructor.
     *
     * @param graphPath full graph path
     * @param nbNodes number of nodes in the graph
     */
    public NodeIdMap(String graphPath, long nbNodes) throws IOException {
        this.graphPath = graphPath;
        this.nbIds = nbNodes;

        int swhToNodeLineLength = 30;
        int nodeToSwhLineLength = 22;
        this.swhToNodeMap = new MapFile(graphPath + Graph.PID_TO_NODE, swhToNodeLineLength);
        this.nodeToSwhMap = new MapFile(graphPath + Graph.NODE_TO_PID, nodeToSwhLineLength);
    }

    /**
     * Converts SWH PID to corresponding long node id.
     *
     * @param swhPID node represented as a {@link SwhPID}
     * @return corresponding node as a long id
     * @see org.softwareheritage.graph.SwhPID
     */
    public long getNodeId(SwhPID swhPID) {
        // The file is sorted by swhPID, hence we can binary search on swhPID to get corresponding
        // nodeId
        long start = 0;
        long end = nbIds - 1;

        while (start <= end) {
            long lineNumber = (start + end) / 2L;
            byte[] entry = swhToNodeMap.readBytesAtLine(lineNumber);
            String currentSwhPID = getSwhPID(entry).getSwhPID();
            long currentNodeId = new BigInteger(entry, 22, 8).longValue();

            int cmp = currentSwhPID.compareTo(swhPID.toString());
            if (cmp == 0) {
                return currentNodeId;
            } else if (cmp < 0) {
                start = lineNumber + 1;
            } else {
                end = lineNumber - 1;
            }
        }

        throw new IllegalArgumentException("Unknown SWH PID: " + swhPID);
    }


    /**
     * Converts a node long id to corresponding SWH PID.
     *
     * @param nodeId node as a long id
     * @return corresponding node as a {@link SwhPID}
     * @see org.softwareheritage.graph.SwhPID
     */
    public SwhPID getSwhPID(long nodeId) {
        // Each line in NODE_TO_PID is formatted as: swhPID
        // The file is ordered by nodeId, meaning node0's swhPID is at line 0, hence we can read the
        // nodeId-th line to get corresponding swhPID
        if (nodeId < 0 || nodeId >= nbIds) {
            throw new IllegalArgumentException("Node id " + nodeId + " should be between 0 and " + nbIds);
        }
        return getSwhPID(nodeToSwhMap.readBytesAtLine(nodeId));
    }

    /**
     * Converts a node long id to corresponding SWH PID.
     *
     * @param content byte representation of a SwhPID
     * @return corresponding node as a {@link SwhPID}
     * @see org.softwareheritage.graph.SwhPID
     */
    private SwhPID getSwhPID(byte[] content) {
        byte version = content[0];
        String type = switch (content[1]) {
            case 0x00 -> "cnt";
            case 0x01 -> "dir";
            case 0x02 -> "ori";
            case 0x03 -> "rel";
            case 0x04 -> "rev";
            case 0x05 -> "snp";
            default -> throw new IllegalArgumentException("Byte " + content[1] + " unknown");
        };
        byte[] hash = new byte[20];
        System.arraycopy(content, 2, hash, 0, 20);
        String swhPID = String.format("swh:%d:%s:%s", version, type, bytesToHex(hash));
        return new SwhPID(swhPID);
    }


    // thanks to https://stackoverflow.com/a/9855338
    private static final char[] HEX_ARRAY = "0123456789abcdef".toCharArray();
    private static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }


    // thanks to https://www.daniweb.com/programming/software-development/code/216874/primitive-types-as-byte-arrays
    private static long toLong(byte[] bytes, int offset) {
        if (bytes == null || offset+8 > bytes.length) {
            return 0x0;
        }
        // (Below) convert to longs before shift because digits
        //         are lost with ints beyond the 32-bit limit
        return (long)(0xff & bytes[offset+0]) << 56  |
               (long)(0xff & bytes[offset+1]) << 48  |
               (long)(0xff & bytes[offset+2]) << 40  |
               (long)(0xff & bytes[offset+3]) << 32  |
               (long)(0xff & bytes[offset+4]) << 24  |
               (long)(0xff & bytes[offset+5]) << 16  |
               (long)(0xff & bytes[offset+6]) << 8   |
               (long)(0xff & bytes[offset+7]) << 0;
    }

    /**
     * Closes the mapping files.
     */
    public void close() throws IOException {
        swhToNodeMap.close();
        nodeToSwhMap.close();
    }
}