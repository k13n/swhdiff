package ch.uzh.ifi.swhdiff;

import org.softwareheritage.graph.SWHID;

public class Revision {
    private final SWHID SWHID;
    private final long nodeId;
    private final long timestamp;
    private final String repositoryIds;

    public Revision(SWHID SWHID, long nodeId, long timestamp, String repositoryIds) {
        this.SWHID = SWHID;
        this.nodeId = nodeId;
        this.timestamp = timestamp;
        this.repositoryIds = repositoryIds;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public SWHID getSWHID() {
        return SWHID;
    }

    public long getNodeId() {
        return nodeId;
    }

    public String getRepositoryIds() {
        return repositoryIds;
    }
}
