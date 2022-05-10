package com.trafficcorp.example.demofunctions.function;
import lombok.Data;
import lombok.NonNull;

@Data
@NonNull
public class Incident {
    private String TrafficReportID;
    private String PublishedDate;
    private String Location;
    private float Latitude;
    private float Longitude;
    private String Address;
    private String Status;
    private String StatusDate;
    private String Title;
}
