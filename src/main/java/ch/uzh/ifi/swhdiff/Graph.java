package ch.uzh.ifi.swhdiff;

import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledImmutableGraph;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator;
import it.unimi.dsi.big.webgraph.labelling.BitStreamArcLabelledImmutableGraph;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.util.PermutedFrontCodedStringList;
import org.softwareheritage.graph.Node;
import org.softwareheritage.graph.SwhPID;
import org.softwareheritage.graph.backend.NodeTypesMap;

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
    private PermutedFrontCodedStringList labelMap;
    /** Mapping between SWH-PID and node ids **/
    private NodeIdMap nodeIdMap;


    public Graph(String path) throws IOException, ClassNotFoundException {
        graph = BVGraph.loadMapped(path);
        graphTransposed = BVGraph.loadMapped(path + "-transposed");
        graphLabelled = BitStreamArcLabelledImmutableGraph.loadOffline(path + "-labelled");
        nodeTypesMap = new NodeTypesMap(path);
        labelMap = (PermutedFrontCodedStringList) BinIO.loadObject(path + "-labels.fcl");
        nodeIdMap = new NodeIdMap(path, graph.numNodes());
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
        ArcLabelledNodeIterator.LabelledArcIterator it = graphLabelled.nodeIterator(srcNode).successors();
        long dst;
        while ((dst = it.nextLong()) >= 0) {
            if (nodeType.isEmpty() || nodeTypesMap.getType(dst) == nodeType.get()) {
                int label = (int) it.label().get();
                int missing = (1 << it.label().fixedWidth()) - 1;
                if (label == missing) {
                    System.err.println("Expected a label between nodes ("+srcNode+","+dst+")");
                } else {
                    String edgeLabel = new String(Base64.getDecoder().decode(labelMap.get(label).toString()));
                    callback.accept(new LabelledEdge(srcNode, dst, edgeLabel));
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


    public long getNodeId(SwhPID pid) {
        return nodeIdMap.getNodeId(pid);
    }


    public SwhPID getSwhPID(long nodeId) {
        return nodeIdMap.getSwhPID(nodeId);
    }


    public void close() throws IOException {
        nodeIdMap.close();
    }
}
