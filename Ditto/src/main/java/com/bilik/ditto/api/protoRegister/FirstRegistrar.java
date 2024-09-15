package com.bilik.ditto.api.protoRegister;

import com.bilik.proto.video.ad.VideoAd;
import com.bilik.ditto.core.type.notPojo.protobuf.ProtobufRegister;
import com.bilik.ditto.api.service.providers.ProtobufTypeResolver.ProtobufRegisterInfo;
import com.bilik.proto.video.ad.VideoAd;

import java.util.Collection;

/**
 * ukazka, ako sa budu musiet registrovat protobufre (moze to byt v ramci externej libky, aby sa
 * nemuseli pushovat kvoli tomu zmeny do samotnej aplikacie)
 *
 * Dynamicke pridavanie protobufrov nebude fungovat, pretoze ParquetProtoWriter/Reader potrebuju
 * mat proto classu v classpathe. Pre ostatne (kafka, json) staci mat descriptor, ktory viem
 * dynamicky ziskat z poskytnuteho proto filu.
 *
 * Teda prirobit podmienky, ze sa da registrovat protobuf dynamicky ale ked sa zvoli ProtoParquet tak sa tieto neberu v uvahu,
 * alebo napisat vlastny protoParquet ... co by bol dost overkill za cenu featury, ktoru v ramci tymu nepotrebujeme
 *
 *
 */
public class FirstRegistrar implements ProtobufRegister {

    @Override
    public void register(Collection<ProtobufRegisterInfo> registrar) {
        registrar.add(
                ProtobufRegisterInfo.builder()
                        .name("VideoAd")
                        .clasz(Video.VideoAd.class)
                        .build());
    }

}
