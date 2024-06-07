package org.apache.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.avro.Schema.Type.*;
import static org.apache.avro.Schema.Type.STRING;
import static org.apache.avro.SchemaCompatibility.SchemaCompatibilityType.*;
import static org.apache.avro.SchemaCompatibility.SchemaIncompatibilityType.*;
import static org.apache.avro.Utils.*;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class TestSchemaCompatibilityModifiedSuite {

  private Schema reader;
  private Schema writer;
  private SchemaCompatibility.SchemaCompatibilityType expectedCompatibilityType;
  private SchemaCompatibility.SchemaIncompatibilityType expectedIncompatibilityType;
  private boolean expectedException;

  public TestSchemaCompatibilityModifiedSuite(Schema readerSchema, Schema writerSchema, SchemaCompatibility.SchemaCompatibilityType expectedCompatibilityType, SchemaCompatibility.SchemaIncompatibilityType expectedIncompatibilityType, boolean expectedException) {
    this.reader = readerSchema;
    this.writer = writerSchema;
    this.expectedCompatibilityType = expectedCompatibilityType;
    this.expectedIncompatibilityType = expectedIncompatibilityType;
    this.expectedException = expectedException;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> getParameters() {
    return Arrays.asList(new Object[][]{

        // Schemi incompatibili --> dopo report di Jacoco
        {getUnion(), getUnionDifferent(), INCOMPATIBLE, MISSING_UNION_BRANCH, false},
        {Schema.create(STRING), getUnionString(), COMPATIBLE, null, false},
        {getEnum("Suit"), getArray(), INCOMPATIBLE, TYPE_MISMATCH, false},

    });
  }


  private static Schema getUnionDifferent(){

    Schema firstType = getRecord("LongList");
    List<Schema> schemas = new ArrayList<>();
    schemas.add(firstType);
    Schema schema = Schema.createUnion(schemas);
    return schema;

  }

  private static Schema getUnionString(){

    Schema firstType = Schema.create(STRING);
    List<Schema> schemas = new ArrayList<>();
    schemas.add(firstType);
    Schema schema = Schema.createUnion(schemas);
    return schema;

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
