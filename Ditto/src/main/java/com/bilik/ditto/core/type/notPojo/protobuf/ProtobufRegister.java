package com.bilik.ditto.core.type.notPojo.protobuf;

import com.bilik.ditto.api.service.providers.ProtobufTypeResolver;

import java.util.Collection;

/**
 * Implemented register has to be in package: ${DEFAULT_PACKAGE}
 * Put all your classes in provided registrar.
 * Key is name, which can be whatever you want, but ideally it is full classname,
 * so there won't be any runtime exception because of duplicating the same key.
 * And that name will be also provided to you over user-interface.
 * Why only specified package will be scanned ?
 * Because we don't want to load and scan every class on classpath.
 * And every class related to this project should be situated in "com.bilik.ditto" package
 */
@FunctionalInterface
public interface ProtobufRegister {

    String DEFAULT_PACKAGE = "cz";

    void register(Collection<ProtobufTypeResolver.ProtobufRegisterInfo> registrar);

}
