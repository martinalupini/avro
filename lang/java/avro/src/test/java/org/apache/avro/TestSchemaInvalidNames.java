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
public class TestSchemaInvalidNames {

  private JsonNode schema;
  private Schema.Names names;
  private Schema expectedSchema;
  private boolean expectedException;

  @Parameters
  public static Collection<Object[]> getParameters() throws JsonProcessingException {
    return Arrays.asList(new Object[][]{

        // Names non valido
        {getJsonNode(TypeJson.NULL), "invalid", Schema.create(NULL), true},
        {getJsonNode(TypeJson.BOOLEAN), "invalid", Schema.create(BOOLEAN), true},
        {getJsonNode(TypeJson.INT), "invalid", Schema.create(INT), true},
        {getJsonNode(TypeJson.LONG), "invalid", Schema.create(LONG), true},
        {getJsonNode(TypeJson.FLOAT), "invalid", Schema.create(FLOAT), true},
        {getJsonNode(TypeJson.DOUBLE), "invalid", Schema.create(DOUBLE), true},
        {getJsonNode(TypeJson.BYTES), "invalid", Schema.create(BYTES), true},
        {getJsonNode(TypeJson.STRING), "invalid", Schema.create(STRING), true},
        {getJsonNode(TypeJson.RECORD), "invalid", getRecord("LongList"), true},
        {getJsonNode(TypeJson.ENUM), "invalid", getEnum("Suit"), true},
        {getJsonNode(TypeJson.ARRAY), "invalid", getArray(), true},
        {getJsonNode(TypeJson.MAP), "invalid", getMap(), true},
        {getJsonNode(TypeJson.UNION), "invalid", getUnion(), true},
        {getJsonNode(TypeJson.FIXED), "invalid", getFixed("md5", 16), true},
        {getJsonNode(TypeJson.INVALID), "invalid", null, true},



    });
  }

  public TestSchemaInvalidNames(JsonNode schema, String names, Schema expectedSchema, boolean expectedException){
    this.schema = schema;
    this.expectedSchema = expectedSchema;
    this.expectedException = expectedException;
    this.names = new MockNames();
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

