package com.bilik.ditto.transformation;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtoMapper {

    private static final Logger logger = LoggerFactory.getLogger(ProtoMapper.class);

    private final String[] path;
    private final int[] fieldIndexes;
    private final Descriptors.Descriptor messageDescriptor;

    public ProtoMapper(Descriptors.Descriptor desc, String flattenName) {
        this.messageDescriptor = desc;
        this.path = flattenName.split("\\.");
        this.fieldIndexes = new int[path.length];
        processFieldDescriptor(desc, 0);
    }

    public String getStringValue(Message message) {
        return getValue(message).toString();
    }

    public Object getValue(Message message) {
        return getValue(message, messageDescriptor, 0);
    }

    // todo find out if fields & descriptors would be better for search value
    private Object getValue(Message message, Descriptors.Descriptor desc, int index) {
        Descriptors.FieldDescriptor fd = desc.findFieldByNumber(fieldIndexes[index]);
        // if the field is repeated we have to use an inner message if there is any and process it
        if (fd.isRepeated() && index + 1 < fieldIndexes.length) {
            Object arrayItem =  message.getRepeatedField(fd, fieldIndexes[++index]);
            // if instance of arrayItem is the Message recursion is required
            return (arrayItem instanceof Message) ?
                    getValue((Message)arrayItem, fd.getMessageType(), ++index) :
                    arrayItem;
        } else if (fd.isRepeated()) {
            // note: to this branch we can get only if we want whole array!
            // second condition convert repeatable into array
            int len = message.getRepeatedFieldCount(fd);
            Object[] array = new Object[len];
            for (int i = 0; i < len; ++i) {
                array[i] = message.getRepeatedField(fd, i);
            }
            return array;
        } else if (fd.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
            return getValue((Message) message.getField(fd), fd.getMessageType(), ++index);
        }

        Object result = message.getField(fd);
        // EnumValueDescriptor might cause infinite recursion for fasterxml's ObjectMapper
        if (result instanceof Descriptors.EnumValueDescriptor) {
            result = ((Descriptors.EnumValueDescriptor) result).getName();
        }
        return result;
    }

    private void processFieldDescriptor(Descriptors.Descriptor desc, int pathIndex) {
        logger.debug("Searching {} index [{}]", desc.getFullName(), pathIndex);
        if (desc == null || pathIndex >= path.length) {
            return;
        }
        boolean isArray = path[pathIndex].startsWith("[");
        if (isArray) {
            fieldIndexes[pathIndex] = Integer.parseInt(path[pathIndex].substring(1, 2));
            pathIndex++;
        }
        for (Descriptors.FieldDescriptor fd: desc.getFields()) {
            if (fd != null && fd.getName().equals(path[pathIndex])) {
                logger.debug("Found fd {} index [{}]", fd, pathIndex);
                fieldIndexes[pathIndex] = fd.getNumber();

                if (Descriptors.FieldDescriptor.Type.MESSAGE.equals(fd.getType())) {
                    // if the field is Message -> recursion is required
                    processFieldDescriptor(fd.getMessageType(), ++pathIndex);
                } else if (fd.isRepeated() && pathIndex + 1 != path.length) {
                    // if there is primitive array we want to parse that too (primitive array is always last possible part of path)
                    this.fieldIndexes[++pathIndex] = Integer.parseInt(path[pathIndex].substring(1, 2)); // substring removes brackets
                }
                return;
            }
        }
    }

    public int[] getFieldIndexes() {
        return fieldIndexes;
    }

    public String[] getPath() {
        return path;
    }

}
