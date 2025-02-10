package io.debezium.server.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import io.debezium.DebeziumException;
import io.debezium.serde.DebeziumSerdes;
import io.debezium.server.iceberg.batchsizewait.InterfaceBatchSizeWait;
import io.debezium.server.iceberg.batchsizewait.NoBatchSizeWait;
import io.debezium.server.iceberg.rpc.OlakeRowsIngester;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.se.SeContainer;
import jakarta.enterprise.inject.se.SeContainerInitializer;
import jakarta.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Dependent
public class OlakeRpcServer {

    protected static final Serde<JsonNode> valSerde = DebeziumSerdes.payloadJson(JsonNode.class);
    protected static final Serde<JsonNode> keySerde = DebeziumSerdes.payloadJson(JsonNode.class);
    private Map<String, String> configMap;
    final static Configuration hadoopConf = new Configuration();
    final static Map<String, String> icebergProperties = new ConcurrentHashMap<>();
    static Catalog icebergCatalog;
    static Deserializer<JsonNode> valDeserializer;
    static Deserializer<JsonNode> keyDeserializer;


    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Please provide a JSON config as an argument.");
            System.exit(1);
        }

        String jsonConfig = args[0];
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> configMap = objectMapper.readValue(jsonConfig, new TypeReference<Map<String, String>>() {
        });

        configMap.forEach(hadoopConf::set);
        icebergProperties.putAll(configMap);
        String catalogName = "iceberg";
        if (configMap.get("catalog-name") != null) {
            catalogName = configMap.get("catalog-name");
        }

        if (configMap.get("table-namespace") == null) {
            throw new Exception("Iceberg table namespace not found");
        }

        icebergCatalog = CatalogUtil.buildIcebergCatalog(catalogName, icebergProperties, hadoopConf);

        // TODO : change this to MaxBatchSizeWait based on config later

        InterfaceBatchSizeWait batchSizeWait = new NoBatchSizeWait();
        batchSizeWait.initizalize();

        // configure and set
        valSerde.configure(Collections.emptyMap(), false);
        valDeserializer = valSerde.deserializer();
        // configure and set
        keySerde.configure(Collections.emptyMap(), true);
        keyDeserializer = keySerde.deserializer();

        OlakeRowsIngester ori;


        // Retrieve a CDI-managed bean from the container
        ori = new OlakeRowsIngester();
        ori.setIcebergNamespace(configMap.get("table-namespace"));
        ori.setIcebergCatalog(icebergCatalog);


        // Build the server to listen on port 50051
        Server server = ServerBuilder.forPort(50051)
                .addService(ori)
                .build()
                .start();

        System.out.println("Server started on port 50051 with configuration: " + configMap);
        server.awaitTermination();
    }


}
