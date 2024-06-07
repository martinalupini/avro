package org.apache.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.apache.avro.Schema.Type.*;
    import static org.apache.avro.Schema.Type.STRING;
import static org.apache.avro.SchemaCompatibility.SchemaCompatibilityType.*;
    import static org.apache.avro.Utils.*;
    import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class TestSchemaCompatibilitySemicompatibility {

  private Schema reader;
  private Schema writer;
  private SchemaCompatibility.SchemaCompatibilityType expectedCompatibilityType;
  private SchemaCompatibility.SchemaIncompatibilityType expectedIncompatibilityType;
  private boolean expectedException;

  public TestSchemaCompatibilitySemicompatibility(Schema readerSchema, Schema writerSchema, SchemaCompatibility.SchemaCompatibilityType expectedCompatibilityType, SchemaCompatibility.SchemaIncompatibilityType expectedIncompatibilityType, boolean expectedException) {
    this.reader = readerSchema;
    this.writer = writerSchema;
    this.expectedCompatibilityType = expectedCompatibilityType;
    this.expectedIncompatibilityType = expectedIncompatibilityType;
    this.expectedException = expectedException;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> getParameters() throws JsonProcessingException {
    return Arrays.asList(new Object[][]{

        {Schema.create(STRING), Schema.create(BYTES), COMPATIBLE, null, false},
        {Schema.create(BYTES), Schema.create(STRING), COMPATIBLE, null, false},
        {Schema.create(FLOAT), Schema.create(INT), COMPATIBLE, null, false},
        {Schema.create(DOUBLE), Schema.create(INT), COMPATIBLE, null, false},
        {Schema.create(DOUBLE), Schema.create(LONG), COMPATIBLE, null, false},
        {Schema.create(LONG), Schema.create(INT), COMPATIBLE, null, false},
        {Schema.create(DOUBLE), Schema.create(FLOAT), COMPATIBLE, null, false},
        {Schema.create(FLOAT), Schema.create(LONG), COMPATIBLE, null, false},

    });
  }


  @Test
  public void testGetCompatibility() {

    try {

      SchemaCompatibility.SchemaPairCompatibility result =  SchemaCompatibility.checkReaderWriterCompatibility(reader, writer);

      assertFalse("An exception was expected",expectedException);

      SchemaCompatibility.SchemaCompatibilityType compatibilityType = result.getResult().getCompatibility();
      assertEquals("CompatibilityType is different", expectedCompatibilityType, compatibilityType);

      if(expectedCompatibilityType == INCOMPATIBLE ) {
        SchemaCompatibility.SchemaIncompatibilityType incompatibilityType = result.getResult().getIncompatibilities().get(0).getType();
        assertEquals("IncompatibilityType is different", expectedIncompatibilityType, incompatibilityType);
      }

    } catch (Exception e) {
      assertTrue("Unexpected exception",expectedException);
    }

  }
}
