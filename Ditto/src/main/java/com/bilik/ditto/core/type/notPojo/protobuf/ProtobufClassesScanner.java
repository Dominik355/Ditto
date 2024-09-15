package com.bilik.ditto.core.type.notPojo.protobuf;

import com.bilik.ditto.api.protoRegister.FirstRegistrar;
import com.bilik.ditto.api.service.providers.ProtobufTypeResolver.ProtobufRegisterInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;

public class ProtobufClassesScanner {

    private static final Logger log = LoggerFactory.getLogger(ProtobufRegister.class);

    /**
     * @return registered protos. If nothing found, returns empty Collection
     *
     * TODO - nevedel najst danu triedu, mozno fuckup s classloaderom, takze zatial takto
     */
    public static Collection<ProtobufRegisterInfo> getRegisteredProtos() {
        Collection<ProtobufRegisterInfo> registered = new HashSet<>();
//        Collection<String> names = new HashSet<>();
//
//        Set<Class<? extends ProtobufRegister>> annotated =
//                new Reflections(DEFAULT_PACKAGE)
//                        .getSubTypesOf(ProtobufRegister.class);
//
//        Object obj;
//        for (Class<? extends ProtobufRegister> clasz : annotated) {
//            log.info("ProtobufRegister class found : {}", clasz.getName());
//
//            Collection<ProtobufRegisterInfo> temp = new HashSet<>();
//
//            try {
//                obj = clasz.getConstructor().newInstance();
//                clasz.getMethod("register", Collection.class).invoke(obj, temp);
//
//            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
//                throw new RuntimeException(e);
//            }
//
//            if (!temp.isEmpty()) {
//                // check for duplicate keys
//                for (ProtobufRegisterInfo info : temp) {
//                    if (names.contains(info.name())) {
//                        throw DittoRuntimeException.of("There can not be 2 registered proto classes with same name [name={}, Class={}]", info.name(), info.clasz());
//                    } else {
//                        log.debug("Protobuf class found [name={}, Class={}]", info.name(), info.clasz());
//                        names.add(info.name());
//                        registered.add(info);
//                    }
//                }
//            }
//        }
//
//        return registered;

        new FirstRegistrar().register(registered);
        return registered;
    }

}
