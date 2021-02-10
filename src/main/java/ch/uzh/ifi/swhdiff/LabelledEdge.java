package ch.uzh.ifi.swhdiff;

public class LabelledEdge {
    private final long src;
    private final long dst;
    private final long label;

    public LabelledEdge(long src, long dst, long label) {
        this.src = src;
        this.dst = dst;
        this.label = label;
    }

    public long getDst() {
        return dst;
    }

    public long getSrc() {
        return src;
    }

    public long getLabel() {
        return label;
    }
}
