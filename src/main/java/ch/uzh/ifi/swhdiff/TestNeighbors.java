package ch.uzh.ifi.swhdiff;

import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledImmutableGraph;
import it.unimi.dsi.big.webgraph.labelling.BitStreamArcLabelledImmutableGraph;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.util.PermutedFrontCodedStringList;
import org.softwareheritage.graph.maps.NodeIdMap;

import java.lang.System;
import java.util.Base64;

class TestNeighbors {
    public static void main(String[] args) throws Exception {
        String path = args[0];
        long src = Long.parseLong(args[1]);
//        long src = 200948;
//        long src = 17341882;

        BVGraph graph1 = BVGraph.loadMapped(path);
        ArcLabelledImmutableGraph graph2 = BitStreamArcLabelledImmutableGraph.load(path + "-labelled");
        NodeIdMap nodeIdMap = new NodeIdMap(path, graph1.numNodes());
        PermutedFrontCodedStringList labelMap = (PermutedFrontCodedStringList) BinIO.loadObject(path + "-labels.fcl");

        long dst;
        System.out.format("Source Node: %d (%s)\n\n", src, nodeIdMap.getSWHID(src));

        System.out.println("BVGraph:");
        int totalbv = 0;
        long startTime = System.currentTimeMillis();
        var it1 = graph1.successors(src);
        while ((dst = it1.nextLong()) >= 0) {
            System.out.format("%d (%s)\n", dst, nodeIdMap.getSWHID(dst));
            totalbv++;
        }
        long endTime = System.currentTimeMillis();
        System.out.format("total: %d\n", totalbv);
        System.out.format("Runtime: %dms\n", (endTime - startTime));

        System.out.println("\nArcLabelledImmutableGraph:");
        startTime = System.currentTimeMillis();
        int totallabel = 0;
        var it2 = graph2.successors(src);
        while ((dst = it2.nextLong()) >= 0) {
            String label = "<missing>";
            int labelId = (int) it2.label().get();
            int missing = (1 << it2.label().fixedWidth()) - 1;
            if (labelId != missing) {
                label = new String(Base64.getDecoder().decode(labelMap.get(labelId).toString()));
            }
            System.out.format("%d (%s, labelId: %d, labelString: '%s')\n", dst, nodeIdMap.getSWHID(dst),
                    labelId, label);
            totallabel++;
        }
        endTime = System.currentTimeMillis();
        System.out.format("total: %d\n", totallabel);
        System.out.format("Runtime: %dms\n", (endTime - startTime));

    }
}
