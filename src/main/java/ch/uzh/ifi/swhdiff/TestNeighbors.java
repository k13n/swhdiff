package ch.uzh.ifi.swhdiff;

import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledImmutableGraph;
import it.unimi.dsi.big.webgraph.labelling.BitStreamArcLabelledImmutableGraph;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.util.PermutedFrontCodedStringList;
import org.softwareheritage.graph.backend.NodeIdMap;

import java.lang.System;
import java.util.Base64;

class TestNeighbors {
    public static void main(String[] args) throws Exception {
        String path = args[0];
        long src = Long.parseLong(args[1]);
        // long src = 200948;
        // long src = 17341882;
        long dst;

        BVGraph graph1 = BVGraph.loadMapped(path);
        ArcLabelledImmutableGraph graph2 = BitStreamArcLabelledImmutableGraph.loadOffline(path + "-labelled");
        NodeIdMap nodeIdMap = new NodeIdMap(path, graph1.numNodes());
        PermutedFrontCodedStringList labelMap = (PermutedFrontCodedStringList) BinIO.loadObject(path + "-labels.fcl");

        System.out.format("Source Node: %d (%s)\n\n", src, nodeIdMap.getSwhPID(src));

        long startTime = System.currentTimeMillis();

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
            String label = "<missing>";
            int labelId = (int) it.label().get();
            int missing = (1 << it.label().fixedWidth()) - 1;
            if (labelId != missing) {
                label = new String(Base64.getDecoder().decode(labelMap.get(labelId).toString()));
            }
            System.out.format("%d (%s, labelId: %d, labelString: '%s')\n", dst, nodeIdMap.getSwhPID(dst),
                    labelId, label);
            totallabel++;
        }
        System.out.format("total: %d\n", totallabel);

        long endTime = System.currentTimeMillis();
        System.out.format("\n\nRuntime: %dms\n", (endTime - startTime));
    }
}

