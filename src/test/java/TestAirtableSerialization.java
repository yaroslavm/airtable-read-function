import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import functions.AirtableReadFunction;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class TestAirtableSerialization {
  private final ObjectMapper objectMapper =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @Test
  public void testSerialization() throws JsonProcessingException {
    final var body =
        ""
            + "{"
            + "    \"records\": ["
            + "        {"
            + "            \"id\": \"rectnixnGXaPvrldU\","
            + "            \"createdTime\": \"2022-09-10T17:49:29.000Z\","
            + "            \"fields\": {"
            + "                \"Name\": \"name 1\","
            + "                \"table link\": ["
            + "                    \"receGQoeEKIxVWvVT\","
            + "                    \"recJZDQqwURAWtdUS\""
            + "                ],"
            + "                \"key\": ["
            + "                    \"key 1\","
            + "                    \"key 2\""
            + "                ]"
            + "            }"
            + "        }"
            + "    ],"
            + "    \"offset\": \"itryg141iFSagBhI8/rectnixnGXaPvrldU\""
            + "}";
    final var airResponse = objectMapper.readValue(body, AirtableReadFunction.AirResponse.class);
    Assert.assertNotNull(airResponse);
    final List<AirtableReadFunction.AirRecord> records = airResponse.getRecords();
    Assert.assertNotNull(records);
    Assert.assertEquals(1, records.size());
    final AirtableReadFunction.AirRecord record = records.get(0);
    Assert.assertNotNull(record);
    final Map<String, Object> fields = record.getFields();
    Assert.assertNotNull(fields);
    Assert.assertEquals(3, fields.size());
    Assert.assertTrue(fields.containsKey("Name"));
    Assert.assertEquals("name 1", fields.get("Name"));
    Assert.assertTrue(fields.containsKey("table link"));
    Assert.assertTrue(fields.containsKey("key"));
    final Object arrayValue = fields.get("key");
    Assert.assertTrue(arrayValue instanceof List);
    Assert.assertEquals("key 1", ((List<?>) arrayValue).get(0));
    Assert.assertEquals("key 2", ((List<?>) arrayValue).get(1));
  }
}
