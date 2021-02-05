package ch.uzh.ifi.swhdiff;

import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledImmutableGraph;
import it.unimi.dsi.big.webgraph.labelling.BitStreamArcLabelledImmutableGraph;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.big.util.FrontCodedStringBigList;
import org.softwareheritage.graph.Node;
import org.softwareheritage.graph.SWHID;
import org.softwareheritage.graph.maps.NodeIdMap;
import org.softwareheritage.graph.maps.NodeTypesMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Optional;
import java.util.function.Consumer;

public class Graph {
    /** Compressed graph stored as a {@link it.unimi.dsi.big.webgraph.BVGraph} */
    private BVGraph graph;
    /** Transposed compressed graph (used for backward traversals) */
    private BVGraph graphTransposed;
    /** Graph with edge labels **/
    private ArcLabelledImmutableGraph graphLabelled;
    /** Mapping long id &rarr; node types */
    private NodeTypesMap nodeTypesMap;
    /** Map to translate IDs to edge-labels **/
    private FrontCodedStringBigList labelMap;
    /** Mapping between SWH-PID and node ids **/
    private NodeIdMap nodeIdMap;
    /** Revision timestamps **/
    private long[][] revisionTimestamps;

    public Graph(String path) throws IOException, ClassNotFoundException {
        graph = BVGraph.loadMapped(path);
        graphTransposed = BVGraph.loadMapped(path + "-transposed");
        graphLabelled = BitStreamArcLabelledImmutableGraph.load(path + "-labelled");
        nodeTypesMap = new NodeTypesMap(path);
        labelMap = (FrontCodedStringBigList) BinIO.loadObject(path + "-labels.fcl");
        nodeIdMap = new NodeIdMap(path, graph.numNodes());
        revisionTimestamps = (long[][]) BinIO.loadLongsBig(path + "-revision_timestamps.bin");
    }


    private void neighbors(BVGraph graph, long srcNode, Optional<Node.Type> nodeType, Consumer<Long> callback) {
        var it = graph.successors(srcNode);
        long dst;
        while ((dst = it.nextLong()) >= 0) {
            if (nodeType.isEmpty() || nodeTypesMap.getType(dst) == nodeType.get()) {
                callback.accept(dst);
            }
        }
    }


    public void successors(long srcNode, Optional<Node.Type> nodeType, Consumer<Long> callback) {
        neighbors(graph, srcNode, nodeType, callback);
    }


    public ArrayList<Long> successors(long srcNode, Optional<Node.Type> nodeType) {
        var nodeIds = new ArrayList<Long>();
        successors(srcNode, nodeType, nodeIds::add);
        return nodeIds;
    }

    public void predecessors(long srcNode, Optional<Node.Type> nodeType, Consumer<Long> callback) {
        neighbors(graphTransposed, srcNode, nodeType, callback);
    }


    public ArrayList<Long> predecessors(long srcNode, Optional<Node.Type> nodeType) {
        var nodeIds = new ArrayList<Long>();
        predecessors(srcNode, nodeType, nodeIds::add);
        return nodeIds;
    }


    public void successorsLabelled(long srcNode, Optional<Node.Type> nodeType, Consumer<LabelledEdge> callback) {
        var it = graphLabelled.successors(srcNode);
        long dst;
        while ((dst = it.nextLong()) >= 0) {
            if (nodeType.isEmpty() || nodeTypesMap.getType(dst) == nodeType.get()) {
                int[] labels = (int[]) it.label().get();
                if (labels.length == 0) {
                    System.err.println("Expected a label between nodes ("+srcNode+","+dst+")");
                } else {
                    for (int label : labels) {
                        callback.accept(new LabelledEdge(srcNode, dst, label));
                    }
                }
            }
        }
    }


    public ArrayList<LabelledEdge> successorsLabelled(long srcNode, Optional<Node.Type> nodeType) {
        var successors = new ArrayList<LabelledEdge>();
        successorsLabelled(srcNode, nodeType, successors::add);
        return successors;
    }


    public ArrayList<LabelledEdge> successorsLabelled(long srcNode, Node.Type... nodeTypes) {
        var successors = new ArrayList<LabelledEdge>();
        if (nodeTypes.length == 0) {
            successorsLabelled(srcNode, Optional.empty(), successors::add);
        } else {
            for (Node.Type nodeType : nodeTypes) {
                successorsLabelled(srcNode, Optional.of(nodeType), successors::add);
            }
        }
        return successors;
    }


    public Node.Type getType(long nodeId) {
        return nodeTypesMap.getType(nodeId);
    }


    public long getNodeId(SWHID pid) {
        return nodeIdMap.getNodeId(pid);
    }


    public SWHID getSWHID(long nodeId) {
        return nodeIdMap.getSWHID(nodeId);
    }


    public String getLabel(int labelId) {
        return new String(Base64.getDecoder().decode(labelMap.get(labelId).toString()));
    }


    public long getTimestamp(long nodeId) {
        return BigArrays.get(revisionTimestamps, nodeId);
    }


    public void close() throws IOException {
        nodeIdMap.close();
    }

    public void readRevisions() {
        var it = graphLabelled.nodeIterator();
        while (it.hasNext()) {
            long nodeId = it.nextLong();
            Node.Type type = getType(nodeId);
            if (type == Node.Type.REV) {
              SWHID id = getSWHID(nodeId);
              long timestamp = getTimestamp(nodeId);
              System.out.println(id.getSWHID() + " " + timestamp + " " + nodeId);
            }
        }
    }
}
