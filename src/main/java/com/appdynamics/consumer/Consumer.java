package com.appdynamics.consumer;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.rabbitmq.client.*;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Consumer {

    /**
     * This method loads the config YAML file
     *
     * @param   path    Path to config file
     * @return  A {@code Map<String, Map<String, ?>>} containing config key value pairs
     */
    private static Map<String, Map<String, ?>> load(String path) {
        Yaml yml = new Yaml();
        try (BufferedReader ip = new BufferedReader(new FileReader(path))) {
            return yml.load(ip);
        } catch (IOException ioe) {
            System.out.println("error loading Yaml!!");
            ioe.printStackTrace();
        }
        return null;
    }

    /**
     * This method consumes from rabbitmq and pushes to Cassandra
     *
     * @param   rhost       RabbitMQ host
     * @param   queueName   RabbitMQ queue name
     * @param   chost       Cassandra host
     * @param   keyspace    Cassandra keyspace
     * @throws  Exception
     */
    private static void consume(String rhost, String queueName, String chost, String keyspace) throws Exception{
        // factory to open connection
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rhost);
        // connects to a broker
        Connection connection = factory.newConnection();
        // new channel instance
        Channel channel = connection.createChannel();
        channel.queueDeclare(queueName, false, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        com.rabbitmq.client.Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                //System.out.println(" [x] Received '");
                List<List<String>> result = (List<List<String>>) deserialize(body);
//                for (List<String> s: result) {
//                    System.out.println(s);
//                }
                pushToCassandra(chost, keyspace, result);
            }
        };
        channel.basicConsume(queueName, true, consumer);
    }

    private static void pushToCassandra(String host, String keyspace, List<List<String>> result) {
//        LoggerContext loggerContext = (LoggerContext)LoggerFactory.getILoggerFactory();
//        Logger rootLogger = loggerContext.getLogger("com.datastax.driver.core.Connection");
//        rootLogger.setLevel(Level.WARN);
        LogManager.getRootLogger().setLevel(Level.WARN);
        LogManager.getLogger("com.datastax.driver.core.Cluster").setLevel(Level.WARN);
        Cluster cluster = Cluster.builder().addContactPoint(host).build();
        Session session = cluster.connect(keyspace);
        for (List<String> row: result) {
            String[] data = row.toArray(new String[0]);
            String query = "INSERT INTO " + keyspace + ".student (uid, id, name, department, course) values (";
            query += UUID.randomUUID() + ", " + data[0] + ", \'" + data[1] + "\', \'" + data[2]
                    + "\', \'" + data[3] + "\')";
            // System.out.println(query);
            session.execute(query);
        }
        session.close();
        cluster.close();
    }

    /**
     * Returns Object representation of a byte array
     *
     * @param   body    {@code byte[]} to be deserialized
     * @return  {@code Object} representation of the input byte array
     */
    private static Object deserialize(byte[] body) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(body)) {
            ObjectInputStream o = new ObjectInputStream(bis);
            return o.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] argv) throws Exception {
        // load config yaml
        Map<String, Map<String, ?>> config = load("./config.yml");
        // get postgres connection properties
        Map<String, ?> rabbitProps = config.get("rabbit_config");
        String rhost = (String) rabbitProps.get("host");
        String queueName = (String) rabbitProps.get("queueName");
        Map<String, ?> casProps = config.get("cassandra_config");
        String chost = (String) casProps.get("host");
        String keyspace = (String) casProps.get("keyspace");
        consume(rhost, queueName, chost, keyspace);
    }
}
