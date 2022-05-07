package com.trafficcorp.example.demofunctions.function;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

public class Passthrough implements Function<byte[], byte[]> {
    public Passthrough() {

    }
    @Override
    public byte[] process(byte[] input, Context context) throws Exception {
        return input;
    }
}