import functions.AirtableReadFunction;
import functions.AirtableReadFunction.AirRecord;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@RequiredArgsConstructor
public class TestAirRecordFilter {
  private final AirRecord record;
  private final Map<String, String> filters;
  private final boolean expectedResult;

  @Test
  public void testPassFilter() {
    Assert.assertEquals(expectedResult, AirtableReadFunction.passFilter(record, filters));
  }

  @Parameterized.Parameters(name = "{index}: {0}, {1}")
  public static Object[][] data() {
    var recordWithEmptyFields = recordWithEmptyFields();
    var recordWithFields = recordWithFields();
    var recordWithoutFields = recordWithoutFields();
    var recordWithEmptyValues = recordWithEmptyValues();
    var recordWithNullValues = recordWithNullValues();
    var validFilters = Map.of("status", "VALID", "name", "John Doe");
    var notMatchingFilters = Map.of("status", "DONE", "name", "John Doe");
    var noFilters = (Map<String, String>) null;
    var emptyFilters = Collections.emptyMap();
    var emptyFilterValues = Map.of("status", "", "name", "");
    var nullFilterValues =
        new HashMap<String, String>() {
          {
            put("status", null);
            put("name", null);
          }
        };
    var additionalNullFilterValues =
        new HashMap<String, String>() {
          {
            put("status", null);
            put("name", null);
            put("date", null);
          }
        };

    return new Object[][] {
      {recordWithEmptyFields, emptyFilters, true},
      {recordWithEmptyFields, noFilters, true},
      {recordWithEmptyFields, validFilters, false},
      {recordWithEmptyFields, notMatchingFilters, false},
      {recordWithEmptyFields, emptyFilterValues, false},
      {recordWithEmptyFields, nullFilterValues, false},
      {recordWithFields, emptyFilters, true},
      {recordWithFields, noFilters, true},
      {recordWithFields, validFilters, true},
      {recordWithFields, notMatchingFilters, false},
      {recordWithFields, emptyFilterValues, false},
      {recordWithFields, nullFilterValues, false},
      {recordWithoutFields, emptyFilters, true},
      {recordWithoutFields, noFilters, true},
      {recordWithoutFields, validFilters, false},
      {recordWithoutFields, notMatchingFilters, false},
      {recordWithoutFields, emptyFilterValues, false},
      {recordWithoutFields, nullFilterValues, false},
      {recordWithEmptyValues, emptyFilters, true},
      {recordWithEmptyValues, noFilters, true},
      {recordWithEmptyValues, validFilters, false},
      {recordWithEmptyValues, notMatchingFilters, false},
      {recordWithEmptyValues, emptyFilterValues, true},
      {recordWithEmptyValues, nullFilterValues, false},
      {recordWithNullValues, emptyFilters, true},
      {recordWithNullValues, noFilters, true},
      {recordWithNullValues, validFilters, false},
      {recordWithNullValues, notMatchingFilters, false},
      {recordWithNullValues, emptyFilterValues, false},
      {recordWithNullValues, nullFilterValues, true},
      {recordWithNullValues, additionalNullFilterValues, false},
      {null, emptyFilters, false},
      {null, noFilters, false},
      {null, validFilters, false},
      {null, notMatchingFilters, false},
      {null, emptyFilterValues, false},
      {null, nullFilterValues, false}
    };
  }

  public static AirRecord recordWithoutFields() {
    return new AirRecord("id", "createdTime", null);
  }

  public static AirRecord recordWithEmptyFields() {
    return new AirRecord("id", "createdTime", Collections.emptyMap());
  }

  public static AirRecord recordWithEmptyValues() {
    return new AirRecord("id", "createdTime", Map.of("status", "", "name", ""));
  }

  public static AirRecord recordWithNullValues() {
    return new AirRecord(
        "id",
        "createdTime",
        new HashMap<>() {
          {
            put("status", null);
            put("name", null);
          }
        });
  }

  public static AirRecord recordWithFields() {
    return new AirRecord("id", "createdTime", Map.of("status", "VALID", "name", "John Doe"));
  }
}
