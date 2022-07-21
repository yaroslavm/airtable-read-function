package functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.collect.Maps;
import com.google.events.cloud.pubsub.v1.Message;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AirtableReadFunction implements BackgroundFunction<Message> {
  private static final ObjectMapper om = new ObjectMapper();

  @Override
  public void accept(final Message message, final Context context) throws Exception {
    final var gcpProject = System.getenv("GCP_PROJECT");
    if (gcpProject == null) {
      throw new IllegalArgumentException("GCP_PROJECT is null");
    }
    final var topicOut = System.getenv("TOPIC_OUT");
    if (topicOut == null) {
      throw new IllegalArgumentException("TOPIC_OUT is null");
    }
    final var airtableToken = System.getenv("AIRTABLE_TOKEN");
    if (airtableToken == null) {
      throw new IllegalArgumentException("AIRTABLE_TOKEN is null");
    }

    log.info(
        "Message arrived with id: {}, publishTime: {}, attributes: {}, context: {}",
        message.getMessageID(),
        message.getPublishTime(),
        message.getAttributes(),
        context);
    if (message.getData() == null) {
      log.warn("field `data` is null, exiting.");
      return;
    }
    final var rawDataDecoded =
        new String(Base64.getDecoder().decode(message.getData()), StandardCharsets.UTF_8);
    log.info("Message data: {}", rawDataDecoded);
    final var event = om.readValue(rawDataDecoded, AirtableTriggerEvent.class);

    final var httpClient =
        HttpClient.newBuilder().connectTimeout(Duration.of(5, ChronoUnit.SECONDS)).build();
    final var url =
        String.format("https://api.airtable.com/v0/%s/%s", event.databaseId, event.tableName);
    final var request =
        HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Authorization", "Bearer " + airtableToken)
            .GET()
            .build();
    final var httpResponse = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    final var body = httpResponse.body();
    final var airResponse = om.readValue(body, AirResponse.class);

    final var records = airResponse.records;
    log.info("Number of records received: {}", records == null ? 0 : records.size());
    if (records == null || records.size() == 0) {
      log.info("No records to process");
      return;
    }

    Publisher publisher = null;

    try {
      publisher = Publisher.newBuilder(ProjectTopicName.of(gcpProject, topicOut)).build();
      for (final AirRecord airRecord : records) {
        final var docReq = new DocumentRequest();
        docReq.templateFile = event.templateFile;
        docReq.attributes = airRecord.fields;
        final var status = airRecord.fields.get("status");
        final var recordId = airRecord.fields.get("ID");
        if (passFilter(airRecord, event.filters)) {
          var byteStr = ByteString.copyFromUtf8(om.writeValueAsString(docReq));
          var pubsubMessage = PubsubMessage.newBuilder().setData(byteStr).build();
          publisher.publish(pubsubMessage).get();
          log.info("Record with id {} / {} has been sent", airRecord.id, recordId);
        } else {
          log.info(
              "Record with id {} / {} has a status to skip: {}", airRecord.id, recordId, status);
        }
      }
    } finally {
      if (publisher != null) {
        publisher.publishAllOutstanding();
        publisher.shutdown();
        publisher.awaitTermination(10, TimeUnit.SECONDS);
      }
    }
  }

  /**
   * Filter AirRecord.fields by key-value filters.
   *
   * @param record the record to filter, not null
   * @param filters Map of fields with values that must be equal to the record
   * @return true if all fields are equal, false otherwise or if record is null
   */
  public static boolean passFilter(final AirRecord record, final Map<String, String> filters) {
    if (record == null) {
      return false;
    }
    if (filters == null || filters.isEmpty()) {
      return true;
    }
    final var fields = record.fields;
    if (fields == null) {
      return false; // because fields map is not empty
    }
    var difference = Maps.difference(fields, filters);
    return difference.entriesDiffering().isEmpty() && difference.entriesOnlyOnRight().isEmpty();
  }

  @Data
  public static class AirtableTriggerEvent {
    String databaseId;
    String tableName;
    String templateFile;
    Map<String, String> filters;
  }

  @Data
  public static class AirResponse {
    List<AirRecord> records;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class AirRecord {
    String id;
    String createdTime;
    Map<String, String> fields;
  }

  @Data
  public static class DocumentRequest {
    String templateFile;
    String targetFile;
    Map<String, String> attributes;
  }
}
