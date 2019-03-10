package org.apache.zookeeper;

import org.apache.zookeeper.data.StatPersisted;
import java.io.Serializable;

/**
 * CSCI 612 - Red Team
 *
 * Wrapper class extends StatPersisted in order to be able to serialize the data
 */
public class StatPersistedSerializable extends StatPersisted implements Serializable {
    public StatPersistedSerializable() {
        super();
    }

    public StatPersistedSerializable(long czxid, long mzxid, long ctime, long mtime, int version,
                                     int cversion, int aversion, long ephemeralOwner, long pzxid) {

        super(czxid, mzxid, ctime, mtime, version, cversion, aversion, ephemeralOwner, pzxid);
    }
}
