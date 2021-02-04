package ch.uzh.ifi.swhdiff;

import org.softwareheritage.graph.Node;
import org.softwareheritage.graph.SWHID;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static ch.uzh.ifi.swhdiff.Differ.NULL_DIR;


public class App {
    private Graph graph;


    void execute(String graphPath, String inputRevisionPath, String outputPath) throws IOException, ClassNotFoundException {
        FileWriter fw = new FileWriter(outputPath);

        graph = new Graph(graphPath);
        Differ differ = new Differ(graph);

        BiConsumer<Revision, HashSet<String>> printer = (revision, paths) -> {
            for (String path : paths)  {
                try {
                    path = sanitizePath(path);
                    // cut of the swh:1:rev: prefix of length 10
                    int prefixLen = 10;
                    String hash = revision.getSWHID().getSWHID().substring(prefixLen);
                    fw.write(String.format("%s;%d;%s;%s\n", path, revision.getTimestamp(), hash, revision.getRepositoryIds()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        readInputRevisions(inputRevisionPath, (revision -> {
            HashSet<String> paths = new HashSet<>();
            try {
                long revRootDirId = fetchRootDirectory(revision.getNodeId());
                var parents = fetchParentRevisions(revision.getSWHID());
                if (parents.isEmpty()) {
                    differ.diff(revRootDirId, NULL_DIR, paths::add);
                } else {
                    for (long parentRevId : parents) {
//                        System.out.format("diff(rev: %s, par: %s)\n", graph.getSWHID(revision.getNodeId()), graph.getSWHID(parentRevId));
                        long parentRevRootDir = fetchRootDirectory(parentRevId);
                        differ.diff(revRootDirId, parentRevRootDir, paths::add);
                    }
                }
                printer.accept(revision, paths);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));

        graph.close();
        fw.close();
    }


    private String sanitizePath(String path) {
        // drop semicolons since we output a CSV file with ; as delimiter
        path = path.replace(";", "");
        path = path.replace("\n", "");
        return path;
    }

    private void readInputRevisions(String fileName, Consumer<Revision> consumer) throws IOException {
        try (Stream<String> stream = Files.lines(Paths.get(fileName))) {
            stream.forEach((line) -> {
                Revision revision = null;
                try {
                    String[] splits = line.split(";");
                    SWHID pid = new SWHID("swh:1:rev:"+splits[0]);
                    long nodeId = graph.getNodeId(pid);
                    long timestamp = Long.parseLong(splits[1]);
                    String repositoryIds = splits[2];
                    revision = new Revision(pid, nodeId, timestamp, repositoryIds);
                } catch (IllegalArgumentException e) {
//                    System.out.println(e.getMessage());
                }
                if (revision != null) {
                    consumer.accept(revision);
                }
            });
        }
    }


    private ArrayList<Long> fetchParentRevisions(SWHID revisionPid) {
        // in the compressed graph a revision points to its parent, not the other way around
        long revisionId = graph.getNodeId(revisionPid);
        return graph.successors(revisionId, Optional.of(Node.Type.REV));
    }


    private long fetchRootDirectory(long revisionNodeId) {
        var neighbors = graph.successors(revisionNodeId, Optional.of(Node.Type.DIR));
        if (neighbors.size() != 1) {
            var SWHID = graph.getSWHID(revisionNodeId);
            throw new IllegalStateException(String.format(
                    "Revision %s (ID %d) points to %d root directories (1 expected)",
                    SWHID.getSWHID(), revisionNodeId, neighbors.size()));
        }
        return neighbors.get(0);
    }


    public static void main(String[] args) throws Exception {
        String graphPath = args[0];
        String inputRevisionPath = args[1];
        String outputPath = args[2];
        new App().execute(graphPath, inputRevisionPath, outputPath);
    }
}
