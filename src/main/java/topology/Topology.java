package topology;

import topology.Cluster;

public interface Topology {
    public Cluster getTopology();
    public void addListener(TopologyChangeListener listener);
    public void removeListener(TopologyChangeListener listener);
}
