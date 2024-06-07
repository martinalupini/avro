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
public class TestSchemaModifiedSuite {

  private JsonNode schema;
  private Schema.Names names;
  private Schema expectedSchema;
  private boolean expectedException;

  @Parameters
  public static Collection<Object[]> getParameters() throws JsonProcessingException {
    return Arrays.asList(new Object[][]{

        // Dopo report Jacoco parseNamesDeclared
        {getJsonNode(TypeJson.ERROR), "valid", getError(), false},
        {getJsonNode(TypeJson.EMPTY_RECORD), "valid", null, true},
        {getJsonNode(TypeJson.EMPTY_ENUM), "valid", null, true},
        {getJsonNode(TypeJson.ENUM_DEFAULT), "valid", getEnumDefault(), false},
        {getJsonNode(TypeJson.FIXED_SIZE_NULL), "valid", null, true},
        {getJsonNode(TypeJson.NUMBER), "valid", null, false},
        {getJsonNode(TypeJson.RECORD_FIELDS_NO_ARRAY), "valid", null, true},
        {getJsonNode(TypeJson.ENUM_SYMBOLS_NO_ARRAY), "valid", null, true},
        {getJsonNode(TypeJson.FIXED_SIZE_NO_INT), "valid", null, true},
        {getJsonNode(TypeJson.INVALID_ARRAY), "valid", null, true},
        // Dopo report Jacoco metodo parseCompleteSchema
        {getJsonNode(TypeJson.TEXTUAL), "valid", null, false},
        // Dopo report PIT sul metodo parseNamesDeclared
        {getJsonNode(TypeJson.LOGICAL_TYPE), "valid", getLogicalTypeSchema(), false},

    });
  }

  public TestSchemaModifiedSuite(JsonNode schema, String names, Schema expectedSchema, boolean expectedException){
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
      assertEquals("Schema is different",expectedSchema, actualSchema);

      // Dopo report PIT sul metodo parseNamesDeclared
      if(expectedSchema != null){
        assertEquals(LogicalTypes.fromSchemaIgnoreInvalid(expectedSchema), actualSchema.getLogicalType());
        System.out.println(actualSchema.getLogicalType());
      }

    } catch (Exception e) {
      assertTrue("Unexpected exception",expectedException);
    }

  }

}

