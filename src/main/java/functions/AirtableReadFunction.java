package functions;

import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import com.google.events.cloud.pubsub.v1.Message;
import com.google.gson.Gson;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AirtableReadFunction implements BackgroundFunction<Message> {
  private static final Gson gson = new Gson();

  @Override
  public void accept(final Message message, final Context context) throws Exception {
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
    final var dataObj = gson.fromJson(rawDataDecoded, Object.class);
    log.info("dataObj received: {}", dataObj);
  }
}
