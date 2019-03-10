package org.apache.zookeeper.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;

//******************************************************
// CSCI 612 - Red Team
//
// Jake Marotta
//
// Serialize will serialize and write a serializable object to file
// Deserialize will deserialize the object written in a file
//******************************************************
public class ObjectSerializer {

    public static void serialize(Path path, Serializable object) {
        try (OutputStream out = Files.newOutputStream(path); ObjectOutputStream oos = new ObjectOutputStream(out)) {
            oos.writeObject(object);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Object deserialize(Path path) {
        try (InputStream in = Files.newInputStream(path); ObjectInputStream ois = new ObjectInputStream(in)) {
            return ois.readObject();
        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
