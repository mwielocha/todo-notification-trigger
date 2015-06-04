package com.cyberdolphins.triggers;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.triggers.ITrigger;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;


/**
 * Created by mwielocha on 04/06/15.
 */
public class TodoNotificationTrigger implements ITrigger {

    private static final Logger logger = LoggerFactory.getLogger(TodoNotificationTrigger.class);

    private final static String EXCHANGE = "todo-notification-exchange";

    private final ConnectionFactory factory;
    private final Connection connection;
    private final Channel channel;

    public TodoNotificationTrigger() throws Exception {
        factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE, "fanout");
    }

    public Collection<Mutation> augment(ByteBuffer partitionKey, ColumnFamily update) {

        Map<String, Object> todo = new HashMap<String, Object>();

        CFMetaData metaData = update.metadata();

        Long userId = LongType.instance.compose(partitionKey);

        for (Cell cell : update) {

            ColumnDefinition columnDefinition = metaData.getColumnDefinition(cell.name());

            if (columnDefinition == null) {
                ByteBuffer first = CompositeType.extractComponent(cell.name().toByteBuffer(), 0);
                boolean done = BooleanType.instance.compose(first);

                ByteBuffer second = CompositeType.extractComponent(cell.name().toByteBuffer(), 1);
                UUID id = TimeUUIDType.instance.compose(second);

                todo.put("done", done);
                todo.put("id", id + "");
            } else {
                String name = UTF8Type.instance.compose(cell.value());
                todo.put("name", name);

            }
        }

        try {

            Map<String, Object> event = new HashMap<String, Object>();
            event.put("userId", userId);

            if(!todo.isEmpty()) {
                event.put("todo", todo);
            }

            channel.basicPublish(EXCHANGE, "", null,
                    JSONObject.toJSONString(event).getBytes());

        } catch (IOException e) {
            logger.error("Error on publish", e);
        }

        return null;
    }

}
