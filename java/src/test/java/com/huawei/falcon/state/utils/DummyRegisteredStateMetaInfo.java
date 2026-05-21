package com.huawei.falcon.state.utils;

import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

public class DummyRegisteredStateMetaInfo extends RegisteredStateMetaInfoBase {

    public DummyRegisteredStateMetaInfo(String name) {
        super(name);
    }

    @Override
    public StateMetaInfoSnapshot snapshot() {
        return null;
    }

    @Override
    public RegisteredStateMetaInfoBase withSerializerUpgradesAllowed() {
        return this;
    }
}

