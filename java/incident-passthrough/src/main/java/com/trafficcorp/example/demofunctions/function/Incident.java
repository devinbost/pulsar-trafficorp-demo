package com.trafficcorp.example.demofunctions.function;
import com.datastax.oss.driver.api.mapper.annotations.*;
import com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention;
import lombok.Data;
import lombok.NonNull;

@Data
@NonNull
@NamingStrategy(convention = NamingConvention.UPPER_CASE)
@Entity(defaultKeyspace = "TRAFFIC")
public class Incident {
    @PartitionKey private String TrafficReportID;
    @ClusteringColumn(3) private String PublishedDate;
    private String Location;
    @ClusteringColumn(1) private float Latitude;
    @ClusteringColumn(2) private float Longitude;
    private String Address;
    private String Status;
    private String StatusDate;
    private String Title;
}
