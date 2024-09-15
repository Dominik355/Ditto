package com.bilik.ditto.core.util;

import com.google.protobuf.AbstractMessage.Builder;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Collectors;

public final class ProtoUtils {

    public static String toJson(MessageOrBuilder messageOrBuilder) throws InvalidProtocolBufferException {
        return JsonFormat.printer().print(messageOrBuilder);
    }

    public static Builder createBuilder(Class<? extends Message> clazz) {
        try {
            // Since we are dealing with a Message type, we can call newBuilder()
            return (Builder) clazz.getMethod("newBuilder").invoke(null);

        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException
                 | NoSuchMethodException | SecurityException e) {
            throw new RuntimeException(e);
        }
    }

    public static Method getParseFromMethod(Class<? extends Message> clazz) {
        try {
            return clazz.getMethod("parseFrom", byte[].class);
        } catch ( IllegalArgumentException | NoSuchMethodException | SecurityException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <T extends Message> T toProto(String json, Class<T> clazz) throws InvalidProtocolBufferException {
        Builder builder = null;
        try {
            // Since we are dealing with a Message type, we can call newBuilder()
            builder = (Builder) clazz.getMethod("newBuilder").invoke(null);

        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException
                 | NoSuchMethodException | SecurityException e) {
            throw new RuntimeException(e);
        }

        JsonFormat.parser()
                .ignoringUnknownFields()
                .merge(json, builder);

        // the instance will be from the build
        return (T) builder.build();
    }


    public static <T extends MessageOrBuilder> T toProto(String json, Message.Builder builder) throws InvalidProtocolBufferException {
        JsonFormat.parser()
                .ignoringUnknownFields()
                .merge(json, builder);
        return (T) builder.build();
    }

    public static <T extends MessageOrBuilder> T toProto(String protoJsonStr, T message){
        try {
            Message.Builder builder = message.getDefaultInstanceForType().toBuilder();
            JsonFormat.parser().ignoringUnknownFields().merge(protoJsonStr,builder);
            T out = (T) builder.build();
            return out;
        }catch(Exception e){
            throw new RuntimeException("Error converting Json to proto", e);
        }
    }

    public static List<Message> asMessages(List<MessageOrBuilder> mobs) {
        return mobs.stream()
                .map(ProtoUtils::asMessage)
                .collect(Collectors.toList());
    }

    public static Message asMessage(MessageOrBuilder mob) {
        if (mob instanceof Message.Builder builder) {
            return builder.build();
        }
        return (Message) mob;
    }
}
