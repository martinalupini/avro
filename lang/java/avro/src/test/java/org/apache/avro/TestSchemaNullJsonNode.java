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
public class TestSchemaNullJsonNode {

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



    });
  }

  public TestSchemaNullJsonNode(JsonNode schema, String names, Schema expectedSchema, boolean expectedException){
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

    } catch (Exception e) {
      System.out.println(e.getMessage());
      assertTrue("Unexpected exception",expectedException);
    }

  }

}

