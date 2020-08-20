package ch.uzh.ifi.swhdiff;

import org.softwareheritage.graph.Node;
import org.softwareheritage.graph.SwhPID;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static ch.uzh.ifi.swhdiff.Differ.NULL_DIR;


public class App {
    private Graph graph;

    void execute(String graphPath, String inputRevisionPath) throws IOException, ClassNotFoundException {
        graph = new Graph(graphPath);
        Differ differ = new Differ(graph);

        readInputRevisions(inputRevisionPath, (revision -> {
            Consumer<String> diffCallback = path -> {
                System.out.println(String.format("%s;%s;%d", revision.getSwhPid().getSwhPID(),
                        path, revision.getTimestamp()));
            };

            try {
                long revRootDirId = fetchRootDirectory(revision.getNodeId());
                var parents = fetchParentRevisions(revision.getSwhPid());
                if (parents.isEmpty()) {
                    differ.diff(revRootDirId, NULL_DIR, true, false, diffCallback);
                } else {
                    for (long parentRevId : parents) {
                        long parentRevRootDir = fetchRootDirectory(parentRevId);
                        differ.diff(revRootDirId, parentRevRootDir, true, true, diffCallback);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));
    }


    private void readInputRevisions(String fileName, Consumer<Revision> consumer) throws IOException {
        try (Stream<String> stream = Files.lines(Paths.get(fileName))) {
            stream.forEach((line) -> {
                Revision revision = null;
                try {
                    String[] splits = line.split(" ");
                    SwhPID pid = new SwhPID(splits[0]);
                    long nodeId = graph.getNodeId(pid);
                    long timestamp = Long.parseLong(splits[1]);
                    revision = new Revision(pid, nodeId, timestamp);
                } catch (IllegalArgumentException e) { }
                if (revision != null) {
                    consumer.accept(revision);
                }
            });
        }
    }


    private ArrayList<Long> fetchParentRevisions(SwhPID revisionPid) {
        long revisionId = graph.getNodeId(revisionPid);
        return graph.predecessors(revisionId, Optional.of(Node.Type.REV));
    }


    private long fetchRootDirectory(long revisionNodeId) {
        var neighbors = graph.successors(revisionNodeId, Optional.of(Node.Type.DIR));
        if (neighbors.size() != 1) {
            var swhPid = graph.getSwhPID(revisionNodeId);
            throw new IllegalStateException(String.format(
                    "Revision %s (ID %d) points to %d root directories (1 expected)",
                    swhPid.getSwhPID(), revisionNodeId, neighbors.size()));
        }
        return neighbors.get(0);
    }


    void sanityCheck(String graphPath, String inputRevisionPath) throws Exception {
        graph = new Graph(graphPath);
        AtomicInteger nrRev = new AtomicInteger(0);
        AtomicInteger nrRevWithRoot = new AtomicInteger(0);
        readInputRevisions(inputRevisionPath, (revision) -> {
            nrRev.incrementAndGet();
            try {
                fetchRootDirectory(revision.getNodeId());
                nrRevWithRoot.incrementAndGet();
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        });
        System.out.println("number of revisions: " + nrRev.get());
        System.out.println("number of revisions with root directory: " + nrRevWithRoot.get());
    }


    public static void main(String[] args) throws Exception {
        String graphPath = args[0];
        String inputRevisionPath = args[1];
        new App().execute(graphPath, inputRevisionPath);
//        new App().sanityCheck(graphPath, inputRevisionPath);
    }
}
