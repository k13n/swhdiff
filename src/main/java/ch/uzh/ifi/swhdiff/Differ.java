package ch.uzh.ifi.swhdiff;

import org.softwareheritage.graph.Node;

import java.util.List;
import java.util.function.Consumer;


public class Differ {
    /** invalid directory id **/
    public static final long NULL_DIR = -1;

    /** buffer that will hold the root-to-leaf edge labels **/
    private static final int MAX_PATH_LEN = 1000;
    private final String[] buffer = new String[MAX_PATH_LEN];
    /** graph to traverse **/
    private final Graph graph;


    public Differ(Graph graph) {
        this.graph = graph;
    }


    public void diff(long dir1, long dir2, boolean dir1Exists, boolean dir2Exists, Consumer<String> callback) {
        diff(dir1,  dir2, dir1Exists, dir2Exists, 0, callback);
    }


    /**
     * Computes the diff between two given directories
     *
     * @param dir1 directory ID of the first tree
     * @param dir2 directory ID of the second tree
     * @param dir1Exists true iff dir1 is a valid directory ID
     * @param dir2Exists true iff dir2 is a valid directory ID
     * @param depth current recursion depth
     * @param callback callback to call when a complete path was found
     */
    private void diff(long dir1, long dir2, boolean dir1Exists, boolean dir2Exists, int depth, Consumer<String> callback) {
        if (!dir1Exists && !dir2Exists) {
            throw new IllegalStateException("at least one directory must exist");
        }
        if (!dir1Exists) {
            // since dir1 does not exist anymore, we collect all leaves in dir2's subtree
            collect(dir2, depth, callback);
            return;
        }
        if (!dir2Exists) {
            // since dir2 does not exist anymore, we collect all leaves in dir1's subtree
            collect(dir1, depth, callback);
            return;
        }
        if (dir1 == dir2) {
            // there's no reason to diff the same two subtrees
            return;
        }

        var children1 = graph.successorsLabelled(dir1, Node.Type.DIR, Node.Type.CNT);
        var children2 = graph.successorsLabelled(dir2, Node.Type.DIR, Node.Type.CNT);

        if (children1.isEmpty() && children2.isEmpty()) {
            // the current node is a leaf-node and can be emitted
            emit(depth, callback);
            return;
        }

        for (var edge1 : children1) {
            buffer[depth] = edge1.getLabel();
            var edge2 = find(children2, edge1);
            if (edge2 == null) {
                // edge1 exists only in dir1
                diff(edge1.getDst(), NULL_DIR, true, false, depth+1, callback);
            } else if (edge1.getDst() != edge2.getDst()) {
                // edge1 exists in dir1 and dir2 and at least one node in their subtrees has changed
                diff(edge1.getDst(), edge2.getDst(), true, true, depth+1, callback);
            }
        }
        for (var edge2 : children2) {
            var edge1 = find(children1, edge2);
            buffer[depth] = edge2.getLabel();
            if (edge1 == null) {
                // edge2 exists only in dir2
                diff(NULL_DIR, edge2.getDst(), false, true, depth+1, callback);
            }
        }
    }


    /**
     * Looks for an occurrence of an edge in a list of edges
     *
     * @param edges list of edges
     * @param refEdge reference edge to find in edges
     * @return the edge in edges that is equal to refEdge
     */
    private LabelledEdge find(List<LabelledEdge> edges, LabelledEdge refEdge) {
        for (var edge : edges) {
            if (refEdge.getLabel().equals(edge.getLabel())) {
                return edge;
            }
        }
        return null;
    }


    /**
     * Collect all paths in the subtree rooted in a given directory
     *
     * @param dir the ID of the current directory
     * @param depth current recursion depth
     * @param callback callback to call when a complete path was found
     */
    private void collect(long dir, int depth, Consumer<String> callback) {
        var children = graph.successorsLabelled(dir, Node.Type.DIR, Node.Type.CNT);
        if (children.isEmpty()) {
            emit(depth, callback);
        } else {
            for (var child : children) {
                buffer[depth] = child.getLabel();
                collect(child.getDst(), depth+1, callback);
            }
        }
    }


    /**
     * Outputs the current path to the callback
     *
     * @param depth current recursion depth
     * @param callback callback to call when a complete path was found
     */
    private void emit(int depth, Consumer<String> callback) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < depth; ++i) {
            sb.append("/");
            sb.append(buffer[i]);
        }
        callback.accept(sb.toString());
    }

}
