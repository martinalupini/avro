package org.apache.avro;

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


}
