package functions;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import com.google.events.cloud.pubsub.v1.Message;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AirtableReadFunction implements BackgroundFunction<Message> {
  private static final ObjectMapper om = new ObjectMapper();

  @Override
  public void accept(final Message message, final Context context) throws Exception {
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
    final var event = om.readValue(rawDataDecoded, AirtableTriggerEvent.class);

    final var httpClient =
        HttpClient.newBuilder().connectTimeout(Duration.of(5, ChronoUnit.SECONDS)).build();
    final var url =
        String.format("https://api.airtable.com/v0/%s/%s", event.databaseId, event.tableName);
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Authorization", "Bearer " + airtableToken)
            .GET()
            .build();
    final var httpResponse = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    final var body = httpResponse.body();
    final var airResponse = om.readValue(body, AirResponse.class);

    log.info(
        "Number of records received: {}",
        airResponse.records == null ? 0 : airResponse.records.size());
  }

  @Data
  public static class AirtableTriggerEvent {
    String databaseId;
    String tableName;
    String targetFile;
    Map<String, String> attributes;
  }

  @Data
  public static class AirResponse {
    List<AirRecord> records;
  }

  @Data
  public static class AirRecord {
    @JsonIgnore Map<String, Object> fields = new HashMap<>();

    @JsonAnyGetter
    public Map<String, Object> getFields() {
      return this.fields;
    }

    @JsonAnySetter
    public void setFields(String name, Object value) {
      this.fields.put(name, value);
    }
  }
}
