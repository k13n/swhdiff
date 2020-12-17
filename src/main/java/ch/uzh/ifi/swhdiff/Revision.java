package ch.uzh.ifi.swhdiff;

import org.softwareheritage.graph.SwhPID;

public class Revision {
    private final SwhPID swhPid;
    private final long nodeId;
    private final long timestamp;
    private final String repositoryIds;

    public Revision(SwhPID swhPid, long nodeId, long timestamp, String repositoryIds) {
        this.swhPid = swhPid;
        this.nodeId = nodeId;
        this.timestamp = timestamp;
        this.repositoryIds = repositoryIds;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public SwhPID getSwhPid() {
        return swhPid;
    }

    public long getNodeId() {
        return nodeId;
    }

    public String getRepositoryIds() {
        return repositoryIds;
    }
}
