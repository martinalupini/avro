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

        // JsonNode null
        {null, "valid", Schema.create(NULL), true},
        {null, "valid", Schema.create(BOOLEAN), true},
        {null, "valid", Schema.create(INT), true},
        {null, "valid", Schema.create(LONG), true},
        {null, "valid", Schema.create(FLOAT), true},
        {null, "valid", Schema.create(DOUBLE), true},
        {null, "valid", Schema.create(BYTES), true},
        {null, "valid", Schema.create(STRING), true},
        {null, "valid", getRecord(), true},
        {null, "valid", getEnum(), true},
        {null, "valid", getArray(), true},
        {null, "valid", getMap(), true},
        {null, "valid", getUnion(), true},
        {null, "valid", getFixed(), true},
        {null, "valid", null, true},

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
      assertEquals(expectedSchema, actualSchema);
      assertFalse("An exception was expected.", expectedException);

    } catch (Exception e) {
      assertTrue(expectedException);
    }

  }


}

