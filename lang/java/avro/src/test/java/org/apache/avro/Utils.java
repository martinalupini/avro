package org.apache.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

public class Utils {

  private static String namespace = "org.apache.avro";

  public static Schema getRecord(){

    Schema longSchema = Schema.create(Schema.Type.LONG);
    Schema.Field field = new Schema.Field("value", longSchema, null, null);
    List<Schema.Field> recordFields = new ArrayList<>();
    recordFields.add(field);

    Schema schema = Schema.createRecord("LongList", null, namespace, false, recordFields);
    schema.addAlias("LinkedLongs");

    return schema;
  }

  public static Schema getEnum(){

    List<String> enumValues = new ArrayList<>();
    enumValues.add("SPADES");
    enumValues.add("HEARTS");
    enumValues.add("DIAMONDS");
    enumValues.add("CLUBS");
    Schema schema = Schema.createEnum("Suit", null, namespace, enumValues);
    return schema;
  }

  public static Schema getArray(){

    Schema elementType = Schema.create(Schema.Type.STRING);
    Schema schema = Schema.createArray(elementType);
    return schema;

  }

  public static Schema getMap(){

    Schema valueType = Schema.create(Schema.Type.LONG);
    Schema schema = Schema.createMap(valueType);
    return schema;

  }

  public static Schema getUnion(){

    Schema firstType = Schema.create(Schema.Type.NULL);
    Schema secondType = Schema.create(Schema.Type.STRING);
    List<Schema> schemas = new ArrayList<>();
    schemas.add(firstType);
    schemas.add(secondType);
    Schema schema = Schema.createUnion(schemas);
    return schema;

  }

  public static Schema getFixed(){
    return Schema.createFixed("md5", null, namespace, 16);
  }



  /*
   *   Per ottenere le istanze valide mi sono basata sulla documentazione presente al sito
   *   https://avro.apache.org/docs/1.11.1/specification/
   */
  public static JsonNode getJsonNode(TypeJson type) throws JsonProcessingException {

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

  public enum TypeJson {
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
