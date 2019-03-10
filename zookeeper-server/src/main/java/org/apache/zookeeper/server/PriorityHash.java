package org.apache.zookeeper.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * CSCI 612 - Red Team
 *
 * PriorityHash is a caching solution for storing DataNodes and their path
 * The PriorityHash uses a ConcurrentHashMap which stores the CacheNodes
 * and a PriorityBlockingQueue which organizes the items in the cache by timestamp.
 * The oldest CacheNode will be at the front of the queue.
 *
 * The PriorityCache is initialized with a max size in MB
 *
 * If there is a cache miss, the PriorityCache will go to a temporary file and
 * find the missing item then store it in the cache. If there is not enough room for the
 * node in cache, it will remove the least recently used items (by popping the queue) until
 * there is room in the cache for the node. The remaining nodes which are not in cache will be
 * written back to the temporary file.
 *
 * The PriorityBlockingQueue and ConcurrentHashMap were chosen so that operations are thread safe.
 * All interactions with the File are in synchronized methods.
 */
public class PriorityHash {

    //**********************************************//
    //                  ATTRIBUTES                  //
    //**********************************************//
    private static final Logger LOG = LoggerFactory.getLogger(PriorityHash.class);
    private static final int QUEUE_INITIAL_CAPACITY = 20;
    private static final String CACHE_FILE_NAME = "dataTreeCache";
    private static final String CACHE_FILE_TYPE = "tmp";

    private final PriorityBlockingQueue<CacheNode> queue;
    private final ConcurrentHashMap<String, CacheNode> map;
    private Path cacheFilePath;
    private double size;
    private double maxSize;
    private enum ActivityType {
        GET_NODE_FROM_FILE_ADD_TO_CACHE,
        ADD_NEW_NODE_TO_CACHE,
        ADD_NODE_TO_FILE,
        REMOVE_NODE_FROM_FILE
    }

    //**********************************************//
    //                 CONSTRUCTORS                 //
    //**********************************************//

    public PriorityHash(double maxSize) {
        queue = new PriorityBlockingQueue<>(QUEUE_INITIAL_CAPACITY, Comparator.comparingLong(CacheNode::getTimestamp));
        map = new ConcurrentHashMap<>();
        size = 0;
        this.maxSize = maxSize;

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
        LOG.warn("Current cache size: " + size);

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
                processCacheDataRequest(path, node, ActivityType.ADD_NODE_TO_FILE); //write node directly to file. Do not cache
            } else {
                processCacheDataRequest(path, node, ActivityType.ADD_NEW_NODE_TO_CACHE); //Add node to cache, replace other nodes if cache is too full
            }
        }
    }

    /**
     * Removes node from the tree
     *
     * If the node is found in cache it is removed there. If it is in file then remove it
     * from the file.
     *
     * @param path - path to the node
     */
    public void remove(String path) {
        CacheNode cacheNode = map.get(path);
        if (cacheNode != null) {
            map.remove(path);
            queue.remove(cacheNode);
            size -= cacheNode.getSizeInMB();
        } else {
            processCacheDataRequest(path, null, ActivityType.REMOVE_NODE_FROM_FILE);
        }
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
        try {
            CacheNode cacheNodeToRemove = queue.remove();
            map.remove(cacheNodeToRemove.getPath(), cacheNodeToRemove);
            size -= cacheNodeToRemove.getSizeInMB();
            return cacheNodeToRemove;
        } catch (Exception e) {
            LOG.error("There was a problem with removing the least recently used item from the queue. Error: " + e.getMessage());
            return null;
        }
    }

    /**
     * ProcessCacheDataRequest
     *
     * Scenarios:
     * 1. Write node to file do not bring node into cache
     * 2. Get node from file and store in cache, do not put a different node into cache
     * 3. Get a node from file and swap it with one in cache
     * 4. Delete a node from the file which wasn't in cache
     *
     * Actual reads/writes from file must be synchronized so that no one else can get in
     *
     * @param path - path to the node
     * @param nodeToSave - the node we want to save to file (or null)
     * @param activityType - The type of action that we are performing
     */
     private synchronized void processCacheDataRequest(String path, DataNode nodeToSave, ActivityType activityType) {
         ConcurrentHashMap<String, CacheNode> fileCacheMap;
         try {
             if (Files.exists(cacheFilePath)) {
                 fileCacheMap = (ConcurrentHashMap<String, CacheNode>) ObjectSerializer.deserialize(cacheFilePath);
             } else {
                 cacheFilePath = Files.createTempFile(CACHE_FILE_NAME, CACHE_FILE_TYPE);
                 fileCacheMap = new ConcurrentHashMap<>();
             }

             if (fileCacheMap != null) {
                 CacheNode fileCacheNode = fileCacheMap.get(path);
                 switch (activityType) {
                     case ADD_NODE_TO_FILE:
                         //Just write the node to the file, don't touch the cache
                         if (nodeToSave != null) {
                             fileCacheMap.put(path, new CacheNode(path, nodeToSave));
                             ObjectSerializer.serialize(cacheFilePath, fileCacheMap);
                             LOG.info("Writing node to file because we have decided to store it directly to the file rather than in cache");
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
                                 LOG.info("Cache was full, I deleted a node with path " + removedNode.getPath() + "... Current size is: " + size);
                             }
                             map.put(path, cacheNode);
                             queue.add(cacheNode);
                             size += cacheNode.getSizeInMB();
                             ObjectSerializer.serialize(cacheFilePath, fileCacheMap);  //write out our updated fileCacheMap
                             LOG.info("I added a new node to cache with path " + cacheNode.getPath());
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
                             map.put(path, fileCacheNode);
                             queue.add(fileCacheNode);
                             size += fileCacheNode.getSizeInMB();

                             //Remove the node that we're caching from the fileCacheMap and then write the updated map to file
                             fileCacheMap.remove(path, fileCacheNode);
                             ObjectSerializer.serialize(cacheFilePath, fileCacheMap);
                             LOG.info("There was a cache miss. I just got the node " + fileCacheNode.getPath() + " from file and put it in cache");
                         } else {
                             LOG.warn("Could not find the requested node in cache or in the file.");
                         }
                         break;
                     case REMOVE_NODE_FROM_FILE:
                         if (fileCacheNode != null) {
                             fileCacheMap.remove(path, fileCacheNode);
                             ObjectSerializer.serialize(cacheFilePath, fileCacheMap);
                         } else {
                             LOG.warn("Could not find the requested node in cache or in the file.");
                         }
                         break;
                     default:
                         LOG.warn("Unsupported cache action was called.");
                 }
             }
         }  catch (IOException e) {
            LOG.error("There was an exception writing the node to disk. Error: " + e.getMessage());
         }
    }
}