package org.apache.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.Collection;

import static org.apache.avro.Schema.Type.*;
import static org.apache.avro.Utils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(value= Parameterized.class)
public class TestSchema {

    private JsonNode schema;
    private Schema.Names names;
    private Schema expectedSchema;
    private Class<Exception> expectedException;

    @Parameters
    public static Collection<Object[]> getParameters() throws JsonProcessingException {
      return Arrays.asList(new Object[][]{

          // null Names (i tipi primitivi non dovrebbero avere namespace
          {getJsonNode(TypeJson.NULL), null, Schema.create(NULL), Exception.class},
          {getJsonNode(TypeJson.BOOLEAN), null, Schema.create(BOOLEAN), Exception.class},
          {getJsonNode(TypeJson.INT), null, Schema.create(INT), Exception.class},
          {getJsonNode(TypeJson.LONG), null, Schema.create(LONG), Exception.class},
          {getJsonNode(TypeJson.FLOAT), null, Schema.create(FLOAT), Exception.class},
          {getJsonNode(TypeJson.DOUBLE), null, Schema.create(DOUBLE), Exception.class},
          {getJsonNode(TypeJson.BYTES), null, Schema.create(BYTES), Exception.class},
          {getJsonNode(TypeJson.STRING), null, Schema.create(STRING), Exception.class},
          {getJsonNode(TypeJson.RECORD), null, getRecord(), Exception.class},
          {getJsonNode(TypeJson.ENUM), null, getEnum(), Exception.class},
          {getJsonNode(TypeJson.ARRAY), null, getArray(), Exception.class},
          {getJsonNode(TypeJson.MAP), null, getMap(), Exception.class},
          {getJsonNode(TypeJson.UNION), null, getUnion(), Exception.class},
          {getJsonNode(TypeJson.FIXED), null, getFixed(), Exception.class},
          {getJsonNode(TypeJson.INVALID), null, null, Exception.class},

          // Names validi (ottenuti tramite il costruttore Names("org.apache.avro")
          {getJsonNode(TypeJson.NULL), new Schema.Names("org.apache.avro"), Schema.create(NULL), Exception.class},
          {getJsonNode(TypeJson.BOOLEAN), new Schema.Names("org.apache.avro"), Schema.create(BOOLEAN), null},
          {getJsonNode(TypeJson.INT), new Schema.Names("org.apache.avro"), Schema.create(INT), null},
          {getJsonNode(TypeJson.LONG), new Schema.Names("org.apache.avro"), Schema.create(LONG), null},
          {getJsonNode(TypeJson.FLOAT), new Schema.Names("org.apache.avro"), Schema.create(FLOAT), null},
          {getJsonNode(TypeJson.DOUBLE), new Schema.Names("org.apache.avro"), Schema.create(DOUBLE), null},
          {getJsonNode(TypeJson.BYTES), new Schema.Names("org.apache.avro"), Schema.create(BYTES), null},
          {getJsonNode(TypeJson.STRING), new Schema.Names("org.apache.avro"), Schema.create(STRING), null},
          {getJsonNode(TypeJson.RECORD), new Schema.Names("org.apache.avro"), getRecord(), null},
          {getJsonNode(TypeJson.ENUM), new Schema.Names("org.apache.avro"), getEnum(), null},
          {getJsonNode(TypeJson.ARRAY), new Schema.Names("org.apache.avro"), getArray(), null},
          {getJsonNode(TypeJson.MAP), new Schema.Names("org.apache.avro"), getMap(), null},
          {getJsonNode(TypeJson.UNION), new Schema.Names("org.apache.avro"), getUnion(), null},
          {getJsonNode(TypeJson.FIXED), new Schema.Names("org.apache.avro"), getFixed(), null},
          {getJsonNode(TypeJson.INVALID), new Schema.Names("org.apache.avro"), null, Exception.class},

          // Names non valido
          {getJsonNode(TypeJson.NULL), getInvalidNames(), Schema.create(NULL), Exception.class},
          {getJsonNode(TypeJson.BOOLEAN), getInvalidNames(), Schema.create(BOOLEAN), Exception.class},
          {getJsonNode(TypeJson.INT), getInvalidNames(), Schema.create(INT), Exception.class},
          {getJsonNode(TypeJson.LONG), getInvalidNames(), Schema.create(LONG), Exception.class},
          {getJsonNode(TypeJson.FLOAT), getInvalidNames(), Schema.create(FLOAT), Exception.class},
          {getJsonNode(TypeJson.DOUBLE), getInvalidNames(), Schema.create(DOUBLE), Exception.class},
          {getJsonNode(TypeJson.BYTES), getInvalidNames(), Schema.create(BYTES), Exception.class},
          {getJsonNode(TypeJson.STRING), getInvalidNames(), Schema.create(STRING), Exception.class},
          {getJsonNode(TypeJson.RECORD), getInvalidNames(), getRecord(), Exception.class},
          {getJsonNode(TypeJson.ENUM), getInvalidNames(), getEnum(), Exception.class},
          {getJsonNode(TypeJson.ARRAY), getInvalidNames(), getArray(), Exception.class},
          {getJsonNode(TypeJson.MAP), getInvalidNames(), getMap(), Exception.class},
          {getJsonNode(TypeJson.UNION), getInvalidNames(), getUnion(), Exception.class},
          {getJsonNode(TypeJson.FIXED), getInvalidNames(), getFixed(), Exception.class},
          {getJsonNode(TypeJson.INVALID), getInvalidNames(), null, Exception.class},


      });
    }

    public TestSchema(JsonNode schema, Schema.Names names, Schema expectedSchema, Class<Exception> expectedException){
        this.schema = schema;
        this.names = names;
        this.expectedSchema = expectedSchema;
        this.expectedException = expectedException;
    }

    @Test
    public void testSchemaParse() {

      if (expectedException != null) {

        assertThrows(expectedException, () -> {
          Schema.parse(schema, names);
        });
      } else {

        Schema actualSchema = Schema.parse(schema, names);

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
