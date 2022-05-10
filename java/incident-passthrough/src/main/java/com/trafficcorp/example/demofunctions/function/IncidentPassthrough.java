package com.trafficcorp.example.demofunctions.function;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

public class IncidentPassthrough implements Function<Incident, Incident> {
    
    public IncidentPassthrough() {
        
    }

    @Override
    public Incident process(Incident input, Context context) throws Exception {
        return input;
    }
}