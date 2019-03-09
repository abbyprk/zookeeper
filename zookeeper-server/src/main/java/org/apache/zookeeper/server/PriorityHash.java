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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;

public class PriorityHash {

    //**********************************************//
    //                  ATTRIBUTES                  //
    //**********************************************//
    private static final Logger LOG = LoggerFactory.getLogger(PriorityHash.class);

    private final PriorityBlockingQueue<CacheNode> queue;
    private final ConcurrentHashMap<String, CacheNode> map;
    private double size;
    private double maxSize;
    private static final int QUEUE_INITIAL_CAPACITY = 20;
    private static final String CACHE_FILE_NAME = "dataTreeCache";
    private static final String CACHE_FILE_TYPE = "tmp";
    private Path cacheFilePath;
    private enum ActivityType {
        GET_NODE_FROM_FILE_ADD_TO_CACHE,
        ADD_NEW_NODE_TO_CACHE,
        ADD_NODE_TO_FILE
    }

    //**********************************************//
    //                 CONSTRUCTORS                 //
    //**********************************************//

    public PriorityHash(double maxSize) {
        queue = new PriorityBlockingQueue<>(QUEUE_INITIAL_CAPACITY, Comparator.comparingLong(CacheNode::getTimestamp));
        map = new ConcurrentHashMap<>();
        size = 0;
        this.maxSize = maxSize;

        //Create our file which will hold the nodes that are not in cache. Initialize it to contain an empty hashmap
        try {
            cacheFilePath = Files.createTempFile(CACHE_FILE_NAME, CACHE_FILE_TYPE);
            ObjectSerializer.serialize(cacheFilePath, new ConcurrentHashMap<String, CacheNode>());
        } catch (IOException e) {
            LOG.error("There was an exception creating the cache file on cache initialization. Error: " + e.getMessage());
        }
    }

    //**********************************************//
    //                PUBLIC METHODS                //
    //**********************************************//

    public boolean contains(String path) {
        return map.containsKey(path);
    }

    public synchronized DataNode get(String path) {
        CacheNode cacheNode = map.get(path);

        //If we don't have the node in cache, look it up in file and add it to cache
        if (cacheNode == null) {
            processCacheDataRequest(path, null, ActivityType.GET_NODE_FROM_FILE_ADD_TO_CACHE);
            cacheNode = map.get(path);

            if (cacheNode == null) {
                LOG.error("Could not find node or add the node to cache");
                return null;
            }
        }

        queue.remove(cacheNode);
        cacheNode.updateTimestamp();
        queue.add(cacheNode);
        return cacheNode.getNode();
    }

    /**
     * Updates a node in the map if it exists, if the node does not exist then add it
     * @param path - path to the node
     * @param node - the DataNode we want to cache
     * @param shouldCacheWhenFull - false indicates that we should write the node to file rather than cache when the cache is full.
     *                            - true indicates that we should write the node to cache and remove the least recently used from the cache
     *                            * This would only be false when we are deserializing the data and filling the cache
     */
    public synchronized void set(String path, DataNode node, boolean shouldCacheWhenFull) {
        CacheNode cacheNode = map.get(path);
        if (cacheNode != null) {
            //TODO: want to make sure that we don't go over the max size
            //Need to make sure that we don't accidentally remove our node from the queue
            cacheNode.updateTimestamp();
            cacheNode.setNode(node);
        } else {
            cacheNode = new CacheNode(path, node);
            boolean cacheFull = size + cacheNode.getSizeInMB() > maxSize;

            if (cacheFull && !shouldCacheWhenFull) {
                //In this case we just want to write it to file instead of cache the node
                processCacheDataRequest(path, node, ActivityType.ADD_NODE_TO_FILE);
            } else {
                processCacheDataRequest(path, node, ActivityType.ADD_NEW_NODE_TO_CACHE); //will add the node to cache and replace nodes if necessary
            }
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

        try {
            Files.delete(cacheFilePath);
        } catch (IOException e) {
            LOG.error("Could not delete cacheFile! Exception: " + e.getMessage());
        }
    }

    public double size() {
        return size;
    }

    //**********************************************//
    //                PRIVATE METHODS               //
    //**********************************************//

    private synchronized CacheNode removeLeastRecent() {
        CacheNode cacheNodeToRemove = queue.remove();
        map.remove(cacheNodeToRemove.getPath(), cacheNodeToRemove);
        size -= cacheNodeToRemove.getSizeInMB();
        return cacheNodeToRemove;
    }

    /**
     * Method for getting an item that doesn't exist in the cache
     *
     * 1. Deserialize the uncached (on file) hashmap
     * 2. Remove the least recently used node from the cache and put it in the "uncached" hashmap
     * 3. Lookup node in the "uncached" hashmap and add it to the cache
     * 4. Remove the node from the "uncached" hashmap so that it will not appear in both places
     * 4. Serialize the "uncached" hashmap back to file.
     *
     * Make sure to add checks around nodes missing from "uncached" hashmap
     * Make sure I/O is synchronized
     *
     * In DataTree.getNode -> remove code that looks at datatree's hashmap
     */

    /**
     * Scenarios:
     * 1. write node to file do not bring node into cache
     * 2. get node from file and store in cache, do not put a different node into cache
     * 3. get a node from file and swap it with one in cache
     *
     * Actual reads/writes from file must be synchronized so that no one else can get in
     */
     private synchronized void processCacheDataRequest(String path, DataNode nodeToSave, ActivityType activityType) {
         ConcurrentHashMap<String, CacheNode> fileCacheMap;
         try {
             //If the file doesn't exist then create it.
             if (!Files.exists(cacheFilePath)) {
                 cacheFilePath = Files.createTempFile(CACHE_FILE_NAME, CACHE_FILE_TYPE);
                 fileCacheMap = new ConcurrentHashMap<>();
             } else {
                 //Get the data from the file
                 fileCacheMap = (ConcurrentHashMap<String, CacheNode>) ObjectSerializer.deserialize(cacheFilePath);
             }

             if (fileCacheMap != null) {
                 CacheNode fileCacheNode = fileCacheMap.get(path);
                 switch (activityType) {
                     //Just add the node to the file, don't swap nodes in cache
                     case ADD_NODE_TO_FILE:
                         if (nodeToSave != null) {
                             fileCacheMap.put(path, new CacheNode(path, nodeToSave));
                             ObjectSerializer.serialize(cacheFilePath, fileCacheMap);
                         }
                         break;
                     case ADD_NEW_NODE_TO_CACHE:
                         //We are trying to add a newly created node, but have to make sure that we do not go over our cache size limit
                         //nodeToSave should not be null.
                         if (nodeToSave != null) {
                             CacheNode cacheNode = new CacheNode(path, nodeToSave);
                             boolean cacheFull = size + cacheNode.getSizeInMB() > maxSize;
                             while (cacheFull) {
                                 //Find node to replace and add it to fileCacheMap. Remove the node we are moving into cache
                                 CacheNode removedNode = removeLeastRecent();
                                 fileCacheMap.put(removedNode.getPath(), removedNode);
                                 cacheFull = size + cacheNode.getSizeInMB() > maxSize;
                             }

                             //Now we can add the node to cache
                             map.put(path, cacheNode);
                             queue.add(cacheNode);
                             size += cacheNode.getSizeInMB();
                             ObjectSerializer.serialize(cacheFilePath, fileCacheMap);  //write out our updated fileCacheMap
                         }
                         break;
                     case GET_NODE_FROM_FILE_ADD_TO_CACHE:
                         //In this case we are trying to find a node does not currently exist in the cache and
                         //we must look it up in the file and then add it to the cache. We will keep removing nodes until it fits within the allocated cache size.
                         //If the fileCacheNode does not exist then we didn't actually find the node we're looking for in file.
                         if (fileCacheNode != null) {
                             //See if we can just add our node to the cache without violating size constraints
                             boolean cacheFull = size + fileCacheNode.getSizeInMB() > maxSize;

                             while (cacheFull) {
                                 //Find node to replace and add it to fileCacheMap.
                                 CacheNode removedNode = removeLeastRecent();
                                 fileCacheMap.put(removedNode.getPath(), removedNode);
                                 cacheFull = size + removedNode.getSizeInMB() > maxSize;
                             }

                             //Now we can add the node to cache
                             map.put(path, fileCacheNode);
                             queue.add(fileCacheNode);
                             size += fileCacheNode.getSizeInMB();

                             //Remove the node that we're caching from the fileCacheMap and then write the updated map to file
                             fileCacheMap.remove(fileCacheNode);
                             ObjectSerializer.serialize(cacheFilePath, fileCacheMap);
                         } else {
                             LOG.warn("Could not find the requested node in cache or in the file.");
                         }
                         break;
                 }
             }
         }  catch (IOException e) {
            LOG.error("There was an exception writing the node to disk. Error: " + e.getMessage());
         }
    }
}
