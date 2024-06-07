package org.apache.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
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
public class TestSchemaNullNames {

  private JsonNode schema;
  private Schema.Names names;
  private Schema expectedSchema;
  private boolean expectedException;

  @Parameters
  public static Collection<Object[]> getParameters() throws JsonProcessingException {
    return Arrays.asList(new Object[][]{

        // null Names
        {getJsonNode(TypeJson.NULL), null, Schema.create(NULL), true},
        {getJsonNode(TypeJson.BOOLEAN), null, Schema.create(BOOLEAN), true},
        {getJsonNode(TypeJson.INT), null, Schema.create(INT), true},
        {getJsonNode(TypeJson.LONG), null, Schema.create(LONG), true},
        {getJsonNode(TypeJson.FLOAT), null, Schema.create(FLOAT), true},
        {getJsonNode(TypeJson.DOUBLE), null, Schema.create(DOUBLE), true},
        {getJsonNode(TypeJson.BYTES), null, Schema.create(BYTES), true},
        {getJsonNode(TypeJson.STRING), null, Schema.create(STRING), true},
        {getJsonNode(TypeJson.RECORD), null, getRecord("LongList"), true},
        {getJsonNode(TypeJson.ENUM), null, getEnum("Suit"), true },
        {getJsonNode(TypeJson.ARRAY), null, getArray(), true},
        {getJsonNode(TypeJson.MAP), null, getMap(), true},
        {getJsonNode(TypeJson.UNION), null, getUnion(), true},
        {getJsonNode(TypeJson.FIXED), null, getFixed("md5", 16), true},
        {getJsonNode(TypeJson.INVALID), null, null, true},

    });
  }

  public TestSchemaNullNames(JsonNode schema, String names, Schema expectedSchema, boolean expectedException){
    this.schema = schema;
    this.expectedSchema = expectedSchema;
    this.expectedException = expectedException;
    this.names = null;
  }

  @Test
  public void testSchemaParse() {

    try {

      Schema actualSchema = Schema.parse(schema, names);
      assertEquals(expectedSchema, actualSchema);
      assertFalse("An exception was expected.", expectedException);

      // Dopo report PIT sul metodo parseNamesDeclared
      if(expectedSchema != null){
        assertEquals(LogicalTypes.fromSchemaIgnoreInvalid(expectedSchema), actualSchema.getLogicalType());
      }

    } catch (Exception e) {
      assertTrue(expectedException);
    }

  }
}
