package org.apache.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import static org.apache.avro.Schema.Type.*;
import static org.apache.avro.Utils.*;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class TestSchema {

  private JsonNode schema;
  private Schema.Names names;
  private Schema expectedSchema;
  private boolean expectedException;

  @Parameters
  public static Collection<Object[]> getParameters() throws JsonProcessingException {
    return Arrays.asList(new Object[][]{

        // null Names (i tipi primitivi non dovrebbero avere namespace
        {getJsonNode(TypeJson.NULL), null, Schema.create(NULL), true},
        {getJsonNode(TypeJson.BOOLEAN), null, Schema.create(BOOLEAN), true},
        {getJsonNode(TypeJson.INT), null, Schema.create(INT), true},
        {getJsonNode(TypeJson.LONG), null, Schema.create(LONG), true},
        {getJsonNode(TypeJson.FLOAT), null, Schema.create(FLOAT), true},
        {getJsonNode(TypeJson.DOUBLE), null, Schema.create(DOUBLE), true},
        {getJsonNode(TypeJson.BYTES), null, Schema.create(BYTES), true},
        {getJsonNode(TypeJson.STRING), null, Schema.create(STRING), true},
        {getJsonNode(TypeJson.RECORD), null, getRecord(), true},
        {getJsonNode(TypeJson.ENUM), null, getEnum(), true },
        {getJsonNode(TypeJson.ARRAY), null, getArray(), true},
        {getJsonNode(TypeJson.MAP), null, getMap(), true},
        {getJsonNode(TypeJson.UNION), null, getUnion(), true},
        {getJsonNode(TypeJson.FIXED), null, getFixed(), true},
        {getJsonNode(TypeJson.INVALID), null, null, true},

        /*
        // Names validi (ottenuti tramite il costruttore Names("org.apache.avro")
        {getJsonNode(TypeJson.NULL), new Schema.Names("org.apache.avro"), Schema.create(NULL), true},
        {getJsonNode(TypeJson.BOOLEAN), new Schema.Names("org.apache.avro"), Schema.create(BOOLEAN), false},
        {getJsonNode(TypeJson.INT), new Schema.Names("org.apache.avro"), Schema.create(INT), false},
        {getJsonNode(TypeJson.LONG), new Schema.Names("org.apache.avro"), Schema.create(LONG), false},
        {getJsonNode(TypeJson.FLOAT), new Schema.Names("org.apache.avro"), Schema.create(FLOAT), false},
        {getJsonNode(TypeJson.DOUBLE), new Schema.Names("org.apache.avro"), Schema.create(DOUBLE), false},
        {getJsonNode(TypeJson.BYTES), new Schema.Names("org.apache.avro"), Schema.create(BYTES), false},
        {getJsonNode(TypeJson.STRING), new Schema.Names("org.apache.avro"), Schema.create(STRING), false},
        {getJsonNode(TypeJson.RECORD), new Schema.Names("org.apache.avro"), getRecord(), false},
        {getJsonNode(TypeJson.ENUM), new Schema.Names("org.apache.avro"), getEnum(), false},
        {getJsonNode(TypeJson.ARRAY), new Schema.Names("org.apache.avro"), getArray(), false},
        {getJsonNode(TypeJson.MAP), new Schema.Names("org.apache.avro"), getMap(), false},
        {getJsonNode(TypeJson.UNION), new Schema.Names("org.apache.avro"), getUnion(), false},
        {getJsonNode(TypeJson.FIXED), new Schema.Names("org.apache.avro"), getFixed(), false},
        {getJsonNode(TypeJson.INVALID), new Schema.Names("org.apache.avro"), null, true},

        // Names non valido
        {getJsonNode(TypeJson.NULL), new MockNames(), Schema.create(NULL), true},
        {getJsonNode(TypeJson.BOOLEAN), new MockNames(), Schema.create(BOOLEAN), true},
        {getJsonNode(TypeJson.INT), new MockNames(), Schema.create(INT), true},
        {getJsonNode(TypeJson.LONG), new MockNames(), Schema.create(LONG), true},
        {getJsonNode(TypeJson.FLOAT), new MockNames(), Schema.create(FLOAT), true},
        {getJsonNode(TypeJson.DOUBLE), new MockNames(), Schema.create(DOUBLE), true},
        {getJsonNode(TypeJson.BYTES), new MockNames(), Schema.create(BYTES), true},
        {getJsonNode(TypeJson.STRING), new MockNames(), Schema.create(STRING), true},
        {getJsonNode(TypeJson.RECORD), new MockNames(), getRecord(), true},
        {getJsonNode(TypeJson.ENUM), new MockNames(), getEnum(), true},
        {getJsonNode(TypeJson.ARRAY), new MockNames(), getArray(), true},
        {getJsonNode(TypeJson.MAP), new MockNames(), getMap(), true},
        {getJsonNode(TypeJson.UNION), new MockNames(), getUnion(), true},
        {getJsonNode(TypeJson.FIXED), new MockNames(), getFixed(), true},
        {getJsonNode(TypeJson.INVALID), new MockNames(), null, true},

         */


    });
  }

  public TestSchema(JsonNode schema, Schema.Names names, Schema expectedSchema, boolean expectedException){
    this.schema = schema;
    this.names = names;
    this.expectedSchema = expectedSchema;
    this.expectedException = expectedException;
  }

  @Test
  public void testSchemaParse() {

    try {

      Schema actualSchema = Schema.parse(schema, names);
      assertEquals(expectedSchema, actualSchema);
      assertFalse("An exception was expected.", expectedException);

    } catch (Exception e) {
      assertTrue(expectedException);
    }

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
