package org.atlasapi.deer.elasticsearch;

import org.joda.time.DateTime;

import java.util.Date;
import java.util.Map;

public class EsBroadcast extends EsObject {

    public final static String ID = "id";
    public final static String CHANNEL = "channel";
    public final static String TRANSMISSION_TIME = "transmissionTime";
    public final static String TRANSMISSION_END_TIME = "transmissionEndTime";
    public final static String TRANSMISSION_TIME_IN_MILLIS = "transmissionTimeInMillis";
    public final static String REPEAT = "repeat";

    public EsBroadcast id(String id) {
        properties.put(ID, id);
        return this;
    }

    public EsBroadcast channel(Long channel) {
        properties.put(CHANNEL, channel);
        return this;
    }

    public EsBroadcast transmissionTime(Date transmissionTime) {
        properties.put(TRANSMISSION_TIME, transmissionTime);
        return this;
    }

    public EsBroadcast transmissionEndTime(Date transmissionEndTime) {
        properties.put(TRANSMISSION_END_TIME, transmissionEndTime);
        return this;
    }

    public EsBroadcast repeat(Boolean repeat) {
        properties.put(REPEAT, repeat);
        return this;
    }

    public static EsBroadcast fromMap(Map<String, Object> map) {
        EsBroadcast broadcast = new EsBroadcast();
        broadcast.id((String) map.get(ID));
        broadcast.channel(((Integer) map.get(CHANNEL)).longValue());
        broadcast.transmissionTime(new DateTime(map.get(TRANSMISSION_TIME)).toDate());
        broadcast.transmissionEndTime(new DateTime(map.get(TRANSMISSION_END_TIME)).toDate());
        broadcast.repeat((Boolean) map.get(REPEAT));
        return broadcast;
    }
}
