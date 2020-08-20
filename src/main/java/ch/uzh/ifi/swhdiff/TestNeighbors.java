package ch.uzh.ifi.swhdiff;

import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledImmutableGraph;
import it.unimi.dsi.big.webgraph.labelling.BitStreamArcLabelledImmutableGraph;
import org.softwareheritage.graph.SwhPID;
import org.softwareheritage.graph.backend.NodeIdMap;

import java.lang.System;
import java.io.IOException;

class TestNeighbors {
    public static void main(String[] args) throws IOException {
        String path = "/Users/kevin/Development/swh_extract/python3kcompress/python3k";
        BVGraph graph1 = BVGraph.loadMapped(path);
        ArcLabelledImmutableGraph graph2 = BitStreamArcLabelledImmutableGraph.loadOffline(path + "-labelled");
        NodeIdMap nodeIdMap = new NodeIdMap(path, graph1.numNodes());

        SwhPID srcPid = new SwhPID("swh:1:dir:7a2b28e75af7ed3f6674a8e6e46477bedfdcd439");
        long src = nodeIdMap.getNodeId(srcPid);
        long dst;
        System.out.format("Source Node: %d (%s)\n\n", src, srcPid);

        System.out.println("BVGraph:");
        int totalbv = 0;
        var it1 = graph1.successors(src);
        while ((dst = it1.nextLong()) >= 0) {
            System.out.format("%d (%s)\n", dst, nodeIdMap.getSwhPID(dst));
            totalbv++;
        }
        System.out.format("total: %d\n", totalbv);

        System.out.println("\nArcLabelledImmutableGraph:");
        int totallabel = 0;
        var it = graph2.nodeIterator(src).successors();
        while ((dst = it.nextLong()) >= 0) {
            System.out.format("%d (%s)\n", dst, nodeIdMap.getSwhPID(dst));
            totallabel++;
        }
        System.out.format("total: %d\n", totallabel);
    }
}

