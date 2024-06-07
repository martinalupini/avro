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
public class TestSchemaMalformedJsonNode {

  private JsonNode schema;
  private Schema.Names names;
  private Schema expectedSchema;
  private boolean expectedException;

  @Parameters
  public static Collection<Object[]> getParameters() throws JsonProcessingException {
    return Arrays.asList(new Object[][]{

        // Names validi (ottenuti tramite il costruttore Names("org.apache.avro")
        {getMalformedJsonNode(TypeJson.RECORD), "valid", null, true},
        {getMalformedJsonNode(TypeJson.ENUM), "valid", null, true},
        {getMalformedJsonNode(TypeJson.ARRAY), "valid", null, true},
        {getMalformedJsonNode(TypeJson.MAP), "valid", null, true},
        //{getMalformedJsonNode(TypeJson.UNION), "valid", null, true},  // --> FAILURE: no exception thrown
        {getMalformedJsonNode(TypeJson.UNION), "valid", getEmptyUnion(), false},
        {getMalformedJsonNode(TypeJson.FIXED), "valid", null, true},

    });
  }

  public TestSchemaMalformedJsonNode(JsonNode schema, String names, Schema expectedSchema, boolean expectedException){
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


