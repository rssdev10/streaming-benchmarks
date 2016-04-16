package kafka.benchmark;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serializer/Deserializer for List<String[]>
 *
 */
public class StringArraySerializer implements Closeable, AutoCloseable,
                                        Serializer<List<String[]>>, Deserializer<List<String[]>>, Serde<List<String[]>> {
    private static final Logger LOG = LoggerFactory.getLogger(AdvertisingTopology.class);
    private static StringArraySerializer instance = null;
    private static final Object obj = new Object();

    private StringArraySerializer() {
    }

    public static StringArraySerializer getInstance() {
        synchronized (obj) {
            if (instance == null) {
                instance = new StringArraySerializer();
            }
        }
        return instance;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, List<String[]> data) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutput out = new ObjectOutputStream(bos)) {
            out.writeObject(data);
            return bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<String[]> deserialize(String topic, byte[] data) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
                ObjectInput in = new ObjectInputStream(bis)) {
            return (List<String[]>) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            LOG.error(e.getMessage());
        }
        return null;
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<List<String[]>> serializer() {
        return getInstance();
    }

    @Override
    public Deserializer<List<String[]>> deserializer() {
        return getInstance();
    }
}
