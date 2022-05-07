package com.trafficcorp.example.demofunctions.function;

import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Query;

@Dao
public interface IncidentDao {
    @Insert
    void save(Incident incident);

    //@Query("INSERT INTO ${keyspaceId}.${tableId}")
    //void insertIfExists(Incident incident);
}
