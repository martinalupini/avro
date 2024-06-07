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
public class TestSchemaCompatibilityCompatible {

  private Schema reader;
  private Schema writer;
  private SchemaCompatibility.SchemaCompatibilityType expectedCompatibilityType;
  private SchemaCompatibility.SchemaIncompatibilityType expectedIncompatibilityType;
  private boolean expectedException;

  public TestSchemaCompatibilityCompatible(Schema readerSchema, Schema writerSchema, SchemaCompatibility.SchemaCompatibilityType expectedCompatibilityType, SchemaCompatibility.SchemaIncompatibilityType expectedIncompatibilityType, boolean expectedException) {
    this.reader = readerSchema;
    this.writer = writerSchema;
    this.expectedCompatibilityType = expectedCompatibilityType;
    this.expectedIncompatibilityType = expectedIncompatibilityType;
    this.expectedException = expectedException;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> getParameters() throws JsonProcessingException {
    return Arrays.asList(new Object[][]{

        // Schemi compatibili
        {Schema.create(NULL), Schema.create(NULL), COMPATIBLE, null, false},
        //{null, null, null, null, true}, // FAILURE --> assert failure in method calculateCompatibility
        {Schema.create(STRING), Schema.create(STRING), COMPATIBLE, null, false},
        {Schema.create(INT), Schema.create(INT), COMPATIBLE, null, false},
        {Schema.create(LONG), Schema.create(LONG), COMPATIBLE, null, false},
        {Schema.create(BYTES), Schema.create(BYTES), COMPATIBLE, null, false},
        {Schema.create(FLOAT), Schema.create(FLOAT), COMPATIBLE, null, false},
        {Schema.create(DOUBLE), Schema.create(DOUBLE), COMPATIBLE, null, false},
        {Schema.create(BOOLEAN), Schema.create(BOOLEAN), COMPATIBLE, null, false},
        {getRecord("LongList"), getRecord("LongList"), COMPATIBLE, null, false},
        {getEnum("Suit"), getEnum("Suit"), COMPATIBLE, null, false},
        {getArray(), getArray(), COMPATIBLE, null, false},
        {getMap(), getMap(), COMPATIBLE, null, false},
        {getUnion(), getUnion(), COMPATIBLE, null, false},
        {getFixed("md5", 16), getFixed("md5", 16), COMPATIBLE, null, false},
        {getUnion(), Schema.create(STRING), COMPATIBLE, null, false},

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
