/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
     * Gets the number of MB for the path and data in the D
     * We are ignoring the other fields on the DataNode.
     */
    double getSizeInMB() {
        double pathSize = path == null ? 0 : path.getBytes().length;
        double nodeDataSize = node == null || node.data == null ? 0 : node.data.length;
        return pathSize + nodeDataSize / MB_CONVERSION;
    }

    @Override
    public String toString() {
        return "Node(" + node.toString() + ")";
    }
}
