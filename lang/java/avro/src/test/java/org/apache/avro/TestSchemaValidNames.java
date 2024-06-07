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
public class TestSchemaValidNames {

  private JsonNode schema;
  private Schema.Names names;
  private Schema expectedSchema;
  private boolean expectedException;

  @Parameters
  public static Collection<Object[]> getParameters() throws JsonProcessingException {
    return Arrays.asList(new Object[][]{

        // Names validi (ottenuti tramite il costruttore Names("org.apache.avro")
        {getJsonNode(TypeJson.NULL), "valid", Schema.create(NULL), true},
        {getJsonNode(TypeJson.BOOLEAN), "valid", Schema.create(BOOLEAN), false},
        {getJsonNode(TypeJson.INT), "valid", Schema.create(INT), false},
        {getJsonNode(TypeJson.LONG), "valid", Schema.create(LONG), false},
        {getJsonNode(TypeJson.FLOAT), "valid", Schema.create(FLOAT), false},
        {getJsonNode(TypeJson.DOUBLE), "valid", Schema.create(DOUBLE), false},
        {getJsonNode(TypeJson.BYTES), "valid", Schema.create(BYTES), false},
        {getJsonNode(TypeJson.STRING), "valid", Schema.create(STRING), false},
        {getJsonNode(TypeJson.RECORD), "valid", getRecord(), false},
        {getJsonNode(TypeJson.ENUM), "valid", getEnum(), false},
        {getJsonNode(TypeJson.ARRAY), "valid", getArray(), false},
        {getJsonNode(TypeJson.MAP), "valid", getMap(), false},
        {getJsonNode(TypeJson.UNION), "valid", getUnion(), false},
        {getJsonNode(TypeJson.FIXED), "valid", getFixed(), false},
        {getJsonNode(TypeJson.INVALID), "valid", null, true},
        {getJsonNode(TypeJson.NON_EXISTENT), "valid", null, true},


    });
  }

  public TestSchemaValidNames(JsonNode schema, String names, Schema expectedSchema, boolean expectedException){
    this.schema = schema;
    this.expectedSchema = expectedSchema;
    this.expectedException = expectedException;
    this.names = new Schema.Names("org.apache.avro");
  }

  @Test
  public void testSchemaParse() {

    try {

      Schema actualSchema = Schema.parse(schema, names);
      assertFalse("An exception was expected.", expectedException);
      assertEquals(expectedSchema, actualSchema);

      // Dopo report PIT sul metodo parseNamesDeclared
      if(expectedSchema != null){
        assertEquals(LogicalTypes.fromSchemaIgnoreInvalid(expectedSchema), actualSchema.getLogicalType());
      }

    } catch (Exception e) {
      System.out.println(e.getMessage());
      assertTrue("Unexpected exception",expectedException);
    }

  }

}

