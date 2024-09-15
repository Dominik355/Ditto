package com.bilik.ditto.transformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * This class allows finding data in HashMap based by string path.
 * Example:
 *  We have proto defined like this:
 *  ```
 *  HashMap SubMessage {
 *      string anotherDescription = value;
 *  }
 *  HashMap = Status {
 *      string description = value;
 *      SubMessage subMessage = value;
 *  }
 *  ```
 *  we can access to data in subMessage via instance of this class like this
 *  `subMessage.anotherDescription`
 *
 *  This class also allows accessing data in repeatable field;
 *  ```
 *  HashMap SubMessage {
 *      string anotherDescription = value;
 *  }
 *  HashMap Status {
 *      string description = value;
 *      repeated SubMessage subMessage = [values];
 *  }
 *  ```
 *  we can access to data via instance of this class like this
 *  `subMessage.[0].anotherDescription` <- this will take always anotherDescription form 0 (first) element of array
 *
 *  Strongly inspired ProtoMapper. Search in nested Map for data based by path.
 *
 */
public class MapMapper {

    private static final Logger logger = LoggerFactory.getLogger(MapMapper.class);

    private final String[] path;

    public MapMapper(String flattenName) {
        this.path = flattenName.split("\\.");
    }

    public String getStringValue(Map<String, Object> message) {
        return getValue(message).toString();
    }

    public Object getValue(Map<String, Object> message) {
        Object searched = message;
        for (String subpath : path) {
            boolean isArray = subpath.startsWith("[");
            int idx = -1;
            if (isArray) {
                // a special case is detected [index]; this index has to be parsed from string and store to field index
                // so it's possible to access data in an array/repeatable field
                idx = Integer.parseInt(subpath.substring(1, 2));
            }
            logger.trace("Here idx: {}", idx);
            if (searched instanceof Map) {
                searched = ((Map<String, Object>) searched).get(subpath);
            } else if (idx != -1 && searched instanceof List) {
                searched = ((List<Object>) searched).get(idx);
                logger.trace("searched is item in list: {}", searched);
            } else {
                logger.trace("searched is object");
            }
        }
        return searched;
    }

    public String[] getPath() {
        return path;
    }

}
