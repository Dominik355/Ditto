package com.bilik.ditto.testCommons;

import proto.test.Event;
import proto.test.Location;
import proto.test.Sensor;
import proto.test.SwitchLevel;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class SensorGenerator {

    private static Random random = new Random();

    private static final String[] sensorNames = {"Sensor1", "Staircase", "Garage", "Kitchen", "Sensor432"};
    private static final String[] eventNames = {"Temperature Changed", "Humidity changed", "Temperature raised critically", "It's almost ocean here"};

    public static final Sensor SENSOR_CONSTANT;
    public static final String SENSOR_CONSTANT_JSON;

    static {
        SENSOR_CONSTANT = Sensor.newBuilder()
                .setName("Sensor Alfa")
                .setTemperature(23)
                .setHumidity(53)
                .setDoor(SwitchLevel.OPEN)
                .setLocation(Location.newBuilder()
                        .setX(231)
                        .setY(-33)
                        .build())
                .setEvent(Event.newBuilder()
                        .setName("Temperature Changed")
                        .setValue(5443)
                        .build())
                .build();

        SENSOR_CONSTANT_JSON = """
                {
                  "name": "Sensor Alfa",
                  "temperature": 23.0,
                  "humidity": 53,
                  "door": "OPEN",
                  "location": {
                    "x": 231,
                    "y": -33
                  },
                  "event": {
                    "name": "Temperature Changed",
                    "value": 5443
                  }
                }
                """;
    }

    public static List<Sensor> getRange(int startInclusive, int endExclusive) {
        return IntStream.range(startInclusive, endExclusive)
                .mapToObj(i -> getSingle())
                .toList();
    }

    public static Stream<Sensor> getRangeStream(int startInclusive, int endExclusive) {
        return IntStream.range(startInclusive, endExclusive)
                .mapToObj(i -> getSingle());
    }

    public static Set<Sensor> getRangeSet(int startInclusive, int endExclusive) {
        return IntStream.range(startInclusive, endExclusive)
                .mapToObj(i -> getSingle())
                .collect(Collectors.toUnmodifiableSet());
    }

    public static Sensor getSingle() {
        return Sensor.newBuilder()
                .setName(getSensorName())
                .setTemperature(getTemperature())
                .setHumidity(getHumidity())
                .setDoor(getSwitchLevel())
                .setLocation(getLocation())
                .setEvent(getEvent())
                .build();
    }

    public static String getSensorName() {
        return sensorNames[random.nextInt(sensorNames.length)];
    }

    public static double getTemperature() {
        return random.nextDouble(40);
    }

    public static int getHumidity() {
        return random.nextInt(70);
    }

    public static SwitchLevel getSwitchLevel() {
        return SwitchLevel.values()[random.nextInt(SwitchLevel.values().length - 1)]; // -1 for 'UNRECOGNIZED'
    }

    public static Location getLocation() {
        return Location.newBuilder()
                .setX(random.nextInt(-500, 500))
                .setY(random.nextInt(-500, 500))
                .build();
    }

    public static Event getEvent() {
        return Event.newBuilder()
                .setName(eventNames[random.nextInt(eventNames.length)])
                .setValue(random.nextInt(100))
                .build();
    }
}
