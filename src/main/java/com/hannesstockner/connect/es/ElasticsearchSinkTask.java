package com.hannesstockner.connect.es;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ElasticsearchSinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(ElasticsearchSinkTask.class);


  private String indexPrefix;
  private final String TYPE = "kafka";

  private Client client;

  @Override
  public void start(Map<String, String> props) {
    final String esHost = props.get(ElasticsearchSinkConnector.ES_HOST);
    indexPrefix = props.get(ElasticsearchSinkConnector.INDEX_PREFIX);
    try {
      client = TransportClient
        .builder()
        .build()
        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(esHost), 9300));



      client
        .admin()
        .indices()
        .preparePutTemplate("kafka_template")
        .setTemplate(indexPrefix + "*")
        .addMapping(TYPE, customMapping())
        .get();
    } catch (UnknownHostException ex) {
      throw new ConnectException("Couldn't connect to es host", ex);
    }
  }

  public XContentBuilder customMapping(){

    XContentBuilder rc = null;
    HashMap<String, String> timestampMapping = new HashMap<String, String>();
    timestampMapping.put("type", "date");

    try {
      rc = jsonBuilder()
              .startObject()
              .startObject(TYPE)
                .field("date_detection", true)
                .field("numeric_detection", true)
                .startObject("properties")
                  .field("es_timestamp", timestampMapping)
                  .startObject("payload")
                    .field("type", "nested")
                    .startObject("properties")
                      .field("date", timestampMapping)
                  .endObject()
                .endObject()
              .endObject()
              .endObject();

    } catch (IOException e) {
      log.warn("Could not build elasticsearch index mapper. Going with defaults");
    }

    return rc;
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    for (SinkRecord record : records) {
      log.info("Processing {}", record.value());

      Map<String, Object> payload = null;
      try {
        payload = toJsonMap(record);
        client.prepareIndex(indexPrefix + record.topic().toLowerCase(), TYPE)
                .setSource(payload)
                .get();
      } catch (IOException e) {
        log.error("Error mapping JSON to map, cannot insert into ES  ", e);
        throw new IllegalStateException(e);
      }


    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
  }

  @Override
  public void stop() {
    client.close();
  }

  @Override
  public String version() {
    return new ElasticsearchSinkConnector().version();
  }

  private Map<String, Object> toJsonMap(SinkRecord record) throws IOException {
    JsonConverter converter = new JsonConverter();
    converter.configure(converterConfigs(), false);
    byte[] value = converter.fromConnectData(record.topic(), record.valueSchema(), record.value());
    ObjectMapper mapper = new ObjectMapper();
    Map<String,Object> returnValue = mapper.readValue(value, new TypeReference<HashMap<String,Object>>() {
    });
    returnValue.put("es_timestamp", new Date().getTime());
    return returnValue;
  }

  private Map<String, Object> converterConfigs() {
    return new HashMap<>();
  }
}
