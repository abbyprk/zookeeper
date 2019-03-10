package org.apache.zookeeper.server;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

import static org.junit.Assert.fail;

//******************************************************
// CSCI 612 - Red Team
//
// Jake Marotta
//
// Tests the ObjectSerializer
//******************************************************
public class ObjectSerializerTest {

    @Test
    public void serialization_test() {
        HashMap<String, Serializable> objectMap = new HashMap<>();
        objectMap.put("first", "1.0");
        objectMap.put("second", 2.0);
        try {
            Path tempPath = Files.createTempFile("serializerTest", "tmp");
            ObjectSerializer.serialize(tempPath, objectMap);
            Object deserializedObject = ObjectSerializer.deserialize(tempPath);
            Assert.assertEquals(objectMap, deserializedObject);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
    }

}