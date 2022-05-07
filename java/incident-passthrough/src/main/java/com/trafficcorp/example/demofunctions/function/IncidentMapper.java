package com.trafficcorp.example.demofunctions.function;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;

@Mapper
public interface IncidentMapper {

    @DaoFactory
    IncidentDao incidentDao(@DaoKeyspace CqlIdentifier keyspace);
}
