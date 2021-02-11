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
import java.time.ZonedDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import static ch.uzh.ifi.swhdiff.Differ.NULL_DIR;


public class App {
    private Graph graph;
    private long totalNrRevisions = 35507702;
    private AtomicLong counter = new AtomicLong(0);
    private Scanner inputScanner;
    private FileWriter outputWriter;

    private class Worker implements Runnable {
        private Differ differ;

        public Worker(Graph graph) {
          this.differ = new Differ(graph);
        }

        @Override
        public void run() {
            HashSet<String> paths = new HashSet<>();
            Revision revision = null;
            while ((revision = readInputRevision()) != null) {
                paths.clear();
                try {
                    long revRootDirId = fetchRootDirectory(revision.getNodeId());
                    var parents = fetchParentRevisions(revision);
                    if (parents.isEmpty()) {
                        differ.diff(revRootDirId, NULL_DIR, paths::add);
                    } else {
                        for (long parentRevId : parents) {
                            long parentRevRootDir = fetchRootDirectory(parentRevId);
                            differ.diff(revRootDirId, parentRevRootDir, paths::add);
                        }
                    }
                    printOutput(revision, paths);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                // track progress
                if (counter.incrementAndGet() % 1000 == 0) {
                  long nrProcessed = counter.get();
                  double ratio = nrProcessed / (double) totalNrRevisions;
                  log(String.format("Processed revisions %d/%d (%.3f)", nrProcessed,
                        totalNrRevisions, ratio));
                }
            }
        }
    }


    synchronized void printOutput(Revision revision, HashSet<String> paths) {
        for (String path : paths)  {
            try {
                path = sanitizePath(path);
                // cut of the swh:1:rev: prefix of length 10
                int prefixLen = 10;
                String hash = revision.getSWHID().getSWHID().substring(prefixLen);
                outputWriter.write(String.format("%s;%d;%s;%s\n", path,
                      revision.getTimestamp(), hash, revision.getNodeId()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    void execute(String inputRevisionPath, String graphPath, String outputPath, int nrThreads)
                 throws IOException, ClassNotFoundException {

        log("start the diff");
        outputWriter = new FileWriter(outputPath);
        inputScanner = new Scanner(new File(inputRevisionPath));

        log("load graph");
        graph = new Graph(graphPath);

        log("start worker threads");
        Thread[] differs = new Thread[nrThreads];
        // initialize threads
        for (int i = 0; i < nrThreads; ++i) {
            differs[i] = new Thread(new Worker(graph));
        }
        // start threads
        for (int i = 0; i < nrThreads; ++i) {
            differs[i].start();
        }
        // join threads
        for (int i = 0; i < nrThreads; ++i) {
            try { differs[i].join(); }
            catch (InterruptedException e) { e.printStackTrace(); }
        }

        log("diffing complete");
        graph.close();
        inputScanner.close();
        outputWriter.close();
    }


    private String sanitizePath(String path) {
        // drop semicolons since we output a CSV file with ; as delimiter
        path = path.replace(";", "");
        path = path.replace("\n", "");
        return path;
    }


    synchronized private Revision readInputRevision() {
        Revision revision = null;
        try {
            if (inputScanner.hasNextLine()) {
                String line = inputScanner.nextLine();
                String[] splits = line.split(" ");
                SWHID pid = new SWHID(splits[0]);
                long timestamp = Long.parseLong(splits[1]);
                long nodeId = Long.parseLong(splits[2]);
                revision = new Revision(pid, nodeId, timestamp, null);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return revision;
    }


    private ArrayList<Long> fetchParentRevisions(Revision revision) {
        // in the compressed graph a revision points to its parent, not the other way around
        return graph.successors(revision.getNodeId(), Optional.of(Node.Type.REV));
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


    private void log(String msg) {
      String date = ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);
      System.out.printf("[%s] %s\n", date, msg);
    }


    public static void main(String[] args) throws Exception {
        // String graphPath = args[0];
        // String inputRevisionPath = args[1];
        // String outputPath = args[2];
        // new App().execute(graphPath, inputRevisionPath, outputPath);
        String revisionPath = "/storage/swh_gitlab_100k/revisions.txt";
        String graphPath = "/storage/swh_gitlab_100k/compress/gitlab-100k";
        String outputPath = "/storage/swh_gitlab_100k/dataset.csv";
        int nrThreads = 1;
        new App().execute(revisionPath, graphPath, outputPath, nrThreads);

    }
}
