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

import java.util.concurrent.ConcurrentHashMap;
import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;

public class PriorityHash {

    //**********************************************//
    //                  ATTRIBUTES                  //
    //**********************************************//

    private final PriorityBlockingQueue<CacheNode> queue;
    private final ConcurrentHashMap<String, CacheNode> map;
    private double size;
    private double maxSize;
    private static final int QUEUE_INITIAL_CAPACITY = 20;

    //**********************************************//
    //                 CONSTRUCTORS                 //
    //**********************************************//

    public PriorityHash(double maxSize) {
        queue = new PriorityBlockingQueue<>(QUEUE_INITIAL_CAPACITY, Comparator.comparingLong(CacheNode::getTimestamp));
        map = new ConcurrentHashMap<>();
        size = 0;
        this.maxSize = maxSize;

        map
    }

    //**********************************************//
    //                PUBLIC METHODS                //
    //**********************************************//

    public boolean contains(String path) {
        return map.containsKey(path);
    }

    public synchronized DataNode get(String path) {
        CacheNode cacheNode = map.get(path);
        if (cacheNode != null) {
            queue.remove(cacheNode);
            cacheNode.updateTimestamp();
            queue.add(cacheNode);
            return cacheNode.getNode();
        }
        return null;
    }

    /**
     * Updates a node in the map if it exists, if the node does not exist then add it
     * @param path
     * @param node
     */
    public synchronized void set(String path, DataNode node) {
        CacheNode cacheNode = map.get(path);
        if (cacheNode != null) {
            //TODO: want to make sure that we don't go over the max size
            //Need to make sure that we don't accidentally remove our node from the queue
            cacheNode.updateTimestamp();
            cacheNode.setNode(node);
        } else {
            cacheNode = new CacheNode(path, node);
            if (size + cacheNode.getSizeInMB() > maxSize) {
                removeLeastRecent();
            }
            map.put(path, cacheNode);
            queue.add(cacheNode);
            size += cacheNode.getSizeInMB();
        }
    }

    /**
     * Removes the node from cache
     * @param path
     */
    public void remove(String path) {
        CacheNode cacheNode = map.get(path);
        map.remove(path);
        queue.remove(cacheNode);
        size -= cacheNode.getSizeInMB();
    }

    public void clear() {
        this.queue.clear();
        this.map.clear();
        this.size = 0;
    }

    public double size() {
        return size;
    }

    //**********************************************//
    //                PRIVATE METHODS               //
    //**********************************************//

    private synchronized void removeLeastRecent() {
        CacheNode cacheNode = queue.remove();
        map.remove(cacheNode.getPath(), cacheNode);
        size -= cacheNode.getSizeInMB();
    }

    /**
     * Method for getting an item that doesn't exist in the cache
     *
     * 1. Deserialize the uncached (on file) hashmap
     * 2. Remove the least recently used node from the cache and put it in the "uncached" hashmap
     * 3. Lookup node in the "uncached" hashmap and add it to the cache
     * 4. Serialize the "uncached" hashmap back to file.
     *
     * Make sure to add checks around nodes missing from "uncached" hashmap
     * Make sure I/O is synchronized
     *
     * In DataTree.getNode -> remove code that looks at datatree's hashmap
     */
}
