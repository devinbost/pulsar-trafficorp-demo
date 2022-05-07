package com.trafficcorp.example.demofunctions.function;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import java.nio.file.Paths;
import java.util.Optional;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;

public class CassandraUpdater implements Function<Incident, Incident> {
    String password, username, keyspace, table;
    CqlSession session;
    public CassandraUpdater(){
        password = username = keyspace = table = "";
        session = null;
    }

    @Override
    public void initialize(Context context) throws Exception {
        Function.super.initialize(context);
        // TODO: Setup driver, connection info from context, etc.
        this.password = getUserConfigValueOrDefault(context, "password", "NOTSET");
        this.username = getUserConfigValueOrDefault(context, "username", "NOTSET");
        this.keyspace = getUserConfigValueOrDefault(context, "keyspace", "NOTSET");
        this.table = getUserConfigValueOrDefault(context, "table", "NOTSET");
        CqlSession session = CqlSession.builder()
                // make sure you change the path to the secure connect bundle below
                .withCloudSecureConnectBundle(Paths.get("/path/to/secure-connect-database_name.zip"))
                .withAuthCredentials(this.username, this.password)
                .withKeyspace(this.keyspace)
                .build();
        this.session = session;
    }

    @Override
    public Incident process(Incident input, Context context) throws Exception {
        try{
            IncidentMapper mapper = new IncidentMapperBuilder(session).build();
            IncidentDao dao = mapper.incidentDao(CqlIdentifier.fromCql(this.keyspace));
            dao.save(input);
        }
        catch (Exception e){
            Logger logger = context.getLogger();
            logger.error("ERROR: Couldn't write message with ID: " + input.getTrafficReportID() +
                    ", error is: " + e);
            // If object already exists:
            return null;
        }
        return input;
    }
    @Override
    public void close() throws Exception {
        this.session.close();
    }

    public static String getUserConfigValueOrDefault(Context ctx, String key, String defaultVal) {
        String value = null;
        Optional obj = ctx.getUserConfigValue(key);
        if(obj != null && obj.isPresent()) {
            value = obj.get().toString();
        }
        else
            value = defaultVal;
        return value;
    }
}
