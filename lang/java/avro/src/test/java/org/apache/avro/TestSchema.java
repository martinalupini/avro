package org.apache.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.stream.Stream;

import static org.apache.avro.Schema.Type.*;
import static org.apache.avro.Utils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestSchema {


  static Stream<Arguments> provideParseArguments() throws JsonProcessingException {
    return Stream.of(

        // null Names (i tipi primitivi non dovrebbero avere namespace
        Arguments.of(getJsonNode(TypeJson.NULL), null, Schema.create(NULL), Exception.class),
        Arguments.of(getJsonNode(TypeJson.BOOLEAN), null, Schema.create(BOOLEAN), Exception.class),
        Arguments.of(getJsonNode(TypeJson.INT), null, Schema.create(INT), Exception.class),
        Arguments.of(getJsonNode(TypeJson.LONG), null, Schema.create(LONG), Exception.class),
        Arguments.of(getJsonNode(TypeJson.FLOAT), null, Schema.create(FLOAT), Exception.class),
        Arguments.of(getJsonNode(TypeJson.DOUBLE), null, Schema.create(DOUBLE), Exception.class),
        Arguments.of(getJsonNode(TypeJson.BYTES), null, Schema.create(BYTES), Exception.class),
        Arguments.of(getJsonNode(TypeJson.STRING), null, Schema.create(STRING), Exception.class),
        Arguments.of(getJsonNode(TypeJson.RECORD), null, getRecord(), Exception.class),
        Arguments.of(getJsonNode(TypeJson.ENUM), null, getEnum(), Exception.class),
        Arguments.of(getJsonNode(TypeJson.ARRAY), null, getArray(), Exception.class),
        Arguments.of(getJsonNode(TypeJson.MAP), null, getMap(), Exception.class),
        Arguments.of(getJsonNode(TypeJson.UNION), null, getUnion(), Exception.class),
        Arguments.of(getJsonNode(TypeJson.FIXED), null, getFixed(), Exception.class),
        Arguments.of(getJsonNode(TypeJson.INVALID), null, null, Exception.class),

        // Names validi (ottenuti tramite il costruttore Names("org.apache.avro")
        Arguments.of(getJsonNode(TypeJson.NULL), new Schema.Names("org.apache.avro"), Schema.create(NULL), Exception.class),
        Arguments.of(getJsonNode(TypeJson.BOOLEAN), new Schema.Names("org.apache.avro"), Schema.create(BOOLEAN), null),
        Arguments.of(getJsonNode(TypeJson.INT), new Schema.Names("org.apache.avro"), Schema.create(INT), null),
        Arguments.of(getJsonNode(TypeJson.LONG), new Schema.Names("org.apache.avro"), Schema.create(LONG), null),
        Arguments.of(getJsonNode(TypeJson.FLOAT), new Schema.Names("org.apache.avro"), Schema.create(FLOAT), null),
        Arguments.of(getJsonNode(TypeJson.DOUBLE), new Schema.Names("org.apache.avro"), Schema.create(DOUBLE), null),
        Arguments.of(getJsonNode(TypeJson.BYTES), new Schema.Names("org.apache.avro"), Schema.create(BYTES), null),
        Arguments.of(getJsonNode(TypeJson.STRING), new Schema.Names("org.apache.avro"), Schema.create(STRING), null),
        Arguments.of(getJsonNode(TypeJson.RECORD), new Schema.Names("org.apache.avro"), getRecord(), null),
        Arguments.of(getJsonNode(TypeJson.ENUM), new Schema.Names("org.apache.avro"), getEnum(), null),
        Arguments.of(getJsonNode(TypeJson.ARRAY), new Schema.Names("org.apache.avro"), getArray(), null),
        Arguments.of(getJsonNode(TypeJson.MAP), new Schema.Names("org.apache.avro"), getMap(), null),
        Arguments.of(getJsonNode(TypeJson.UNION), new Schema.Names("org.apache.avro"), getUnion(), null),
        Arguments.of(getJsonNode(TypeJson.FIXED), new Schema.Names("org.apache.avro"), getFixed(), null),
        Arguments.of(getJsonNode(TypeJson.INVALID), new Schema.Names("org.apache.avro"), null, Exception.class),

        // Names non valido
        Arguments.of(getJsonNode(TypeJson.NULL), getInvalidNames(), Schema.create(NULL), Exception.class),
        Arguments.of(getJsonNode(TypeJson.BOOLEAN), getInvalidNames(), Schema.create(BOOLEAN), Exception.class),
        Arguments.of(getJsonNode(TypeJson.INT), getInvalidNames(), Schema.create(INT), Exception.class),
        Arguments.of(getJsonNode(TypeJson.LONG), getInvalidNames(), Schema.create(LONG), Exception.class),
        Arguments.of(getJsonNode(TypeJson.FLOAT), getInvalidNames(), Schema.create(FLOAT), Exception.class),
        Arguments.of(getJsonNode(TypeJson.DOUBLE), getInvalidNames(), Schema.create(DOUBLE), Exception.class),
        Arguments.of(getJsonNode(TypeJson.BYTES), getInvalidNames(), Schema.create(BYTES), Exception.class),
        Arguments.of(getJsonNode(TypeJson.STRING), getInvalidNames(), Schema.create(STRING), Exception.class),
        Arguments.of(getJsonNode(TypeJson.RECORD), getInvalidNames(), getRecord(), Exception.class),
        Arguments.of(getJsonNode(TypeJson.ENUM), getInvalidNames(), getEnum(), Exception.class),
        Arguments.of(getJsonNode(TypeJson.ARRAY), getInvalidNames(), getArray(), Exception.class),
        Arguments.of(getJsonNode(TypeJson.MAP), getInvalidNames(), getMap(), Exception.class),
        Arguments.of(getJsonNode(TypeJson.UNION), getInvalidNames(), getUnion(), Exception.class),
        Arguments.of(getJsonNode(TypeJson.FIXED), getInvalidNames(), getFixed(), Exception.class),
        Arguments.of(getJsonNode(TypeJson.INVALID), getInvalidNames(), null, Exception.class)

    );
  }

  @ParameterizedTest
  @MethodSource("provideParseArguments")
  public void testSchemaParse(JsonNode schema, Schema.Names names, Schema expectedSchema, Class<Exception> expectedException) {

    if (expectedException != null) {

      assertThrows(expectedException, () -> {
        Schema.parse(schema, names);
      });
    } else {

      Schema actualSchema =Schema.parse(schema, names);

      assertEquals(expectedSchema, actualSchema);
    }

  }


  private static Schema.Names getInvalidNames(){
    Schema.Names name = mock(Schema.Names.class);
    when(name.get(anyString())).then((Answer<String>) invocation -> {
      throw new Exception("get not available");
    });

    return name;

  }


  /*
  *   Per ottenere le istanze valide mi sono basata sulla documentazione presente al sito
  *   https://avro.apache.org/docs/1.11.1/specification/
  */
  private static JsonNode getJsonNode(TypeJson type) throws JsonProcessingException {

    JsonNode jsonNode = null;
    ObjectMapper mapper = new ObjectMapper();
    String str;

    switch (type) {

    case NULL:
      jsonNode = null;
      break;

    case BOOLEAN:
      str = "{\"type\":\"boolean\"}";
      jsonNode = mapper.readTree(str);
      break;

    case INT:
      str = "{\"type\":\"int\"}";
      jsonNode = mapper.readTree(str);
      break;

    case LONG:
      str = "{\"type\":\"long\"}";
      jsonNode = mapper.readTree(str);
      break;

    case FLOAT:
      str = "{\"type\":\"float\"}";
      jsonNode = mapper.readTree(str);
      break;

    case DOUBLE:
      str = "{\"type\":\"double\"}";
      jsonNode = mapper.readTree(str);
      break;

    case BYTES:
      str = "{\"type\":\"bytes\"}";
      jsonNode = mapper.readTree(str);
      break;

    case STRING:
      str = "{\"type\":\"string\"}";
      jsonNode = mapper.readTree(str);
      break;

    case RECORD:
      str = "{" +
          "\"type\":\"record\"," +
          "\"name\":\"LongList\"," +
          "\"aliases\":[\"LinkedLongs\"]," +
          "\"fields\":[" +
          "{\"name\":\"value\",\"type\":\"long\"}" +
          "]}";
      jsonNode = mapper.readTree(str);
      break;

    case ENUM:
      str = "{" +
          "\"type\":\"enum\"," +
          "\"name\":\"Suit\"," +
          "\"symbols\":[\"SPADES\",\"HEARTS\",\"DIAMONDS\",\"CLUBS\"]" +
          "}";
      jsonNode = mapper.readTree(str);
      break;

    case ARRAY:
      str = "{" +
          "\"type\":\"array\"," +
          "\"items\":\"string\"" +
          "}";
      jsonNode = mapper.readTree(str);
      break;

    case MAP:
      str = "{" +
          "\"type\":\"map\"," +
          "\"values\":\"long\"" +
          "}";
      jsonNode = mapper.readTree(str);
      break;

    case UNION:
      str = "[\"null\",\"string\"]";
      jsonNode = mapper.readTree(str);
      break;

    case FIXED:
      str = "{\"type\":\"fixed\",\"size\":16,\"name\":\"md5\"}";
      jsonNode = mapper.readTree(str);
      break;

    case INVALID:
      str = "{}";
      jsonNode = mapper.readTree(str);
      break;

    }

    return jsonNode;

  }

  private enum TypeJson {
    NULL,
    BOOLEAN,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    BYTES,
    STRING,
    RECORD,
    ENUM,
    ARRAY,
    MAP,
    UNION,
    FIXED,
    INVALID
  }
}
