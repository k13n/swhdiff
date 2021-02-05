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

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.Properties;


public class RevisionReader {

    public static void main(String[] args) throws Exception {
        String graphPath = args[0];
        Properties properties = new Properties();
        properties.load(new FileInputStream(graphPath + ".properties"));
        long nrNodes = Long.parseLong(properties.getProperty("nodes"));

        NodeTypesMap nodeTypesMap = new NodeTypesMap(graphPath);;
        NodeIdMap nodeIdMap = new NodeIdMap(graphPath, nrNodes);
        long[][] revisionTimestamps = (long[][]) BinIO.loadLongsBig(graphPath + "-revision_timestamps.bin");

        for (long nodeId = 0; nodeId < nrNodes; ++nodeId) {
            Node.Type type = nodeTypesMap.getType(nodeId);
            if (true || type == Node.Type.REV) {
              SWHID id = nodeIdMap.getSWHID(nodeId);
              long timestamp = BigArrays.get(revisionTimestamps, nodeId);
              System.out.println(id.getSWHID() + " " + timestamp + " " + nodeId);
            }
        }
    }
}
