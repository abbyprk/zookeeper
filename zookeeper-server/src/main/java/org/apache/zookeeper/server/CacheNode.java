/**
 * CSCI 612 - Red Team
 *
 * Cache Node keeps track of the DataNode and the path to the node within the tree.
 * Cache node contains a timestamp which indicates when the node was last referenced or updated.
 * The node is serializable
 */
package org.apache.zookeeper.server;

import java.io.Serializable;

public class CacheNode implements Serializable {

    private String path;
    private long timestamp;
    private DataNode node;
    private static final double MB_CONVERSION = 1048576;
    private static final long serialVersionUID = -11111111;

    CacheNode(String path, DataNode node) {
        this.path = path;
        this.node = node;
        this.timestamp = System.nanoTime();
    }

    String getPath() {
        return path;
    }

    long getTimestamp() {
        return timestamp;
    }

    void updateTimestamp() {
        timestamp = System.nanoTime();
    }

    DataNode getNode() {
        return node;
    }

    void setNode(DataNode node) {
        this.node = node;
    }

    /**
     * Gets the number of MB for the path and data in the DataNode
     * @return size of data and path in MB
     */
    double getSizeInMB() {
        double pathSize = path == null ? 0 : path.getBytes().length;
        double nodeDataSize = node == null || node.data == null ? 0 : node.data.length;
        return (pathSize + nodeDataSize) / MB_CONVERSION;
    }

    @Override
    public String toString() {
        return "Node(" + node.toString() + ")";
    }
}
