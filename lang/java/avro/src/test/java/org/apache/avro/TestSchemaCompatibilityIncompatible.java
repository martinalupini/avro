package org.apache.avro;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Answers;

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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class TestSchemaCompatibilityIncompatible {

  private Schema reader;
  private Schema writer;
  private SchemaCompatibility.SchemaCompatibilityType expectedCompatibilityType;
  private SchemaCompatibility.SchemaIncompatibilityType expectedIncompatibilityType;
  private boolean expectedException;

  public TestSchemaCompatibilityIncompatible(Schema readerSchema, Schema writerSchema, SchemaCompatibility.SchemaCompatibilityType expectedCompatibilityType, SchemaCompatibility.SchemaIncompatibilityType expectedIncompatibilityType, boolean expectedException) {
    this.reader = readerSchema;
    this.writer = writerSchema;
    this.expectedCompatibilityType = expectedCompatibilityType;
    this.expectedIncompatibilityType = expectedIncompatibilityType;
    this.expectedException = expectedException;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> getParameters(){
    return Arrays.asList(new Object[][]{

        // Name mismatch
        {getRecord("LongList"), getRecord("Record"), INCOMPATIBLE, NAME_MISMATCH, false},
        {getEnum("Suit"), getEnum("Enum"), INCOMPATIBLE, NAME_MISMATCH, false},
        {getFixed("md5", 16), getFixed("Fixed", 16), INCOMPATIBLE, NAME_MISMATCH, false},

        // Fixed size mismatch
        {getFixed("md5", 16), getFixed("md5", 15), INCOMPATIBLE, FIXED_SIZE_MISMATCH, false},

        // missing enum symbols
        { getIncompleteEnum("Suit"),getEnum("Suit"), INCOMPATIBLE, MISSING_ENUM_SYMBOLS, false },

        // missing default value reader
        { getRecord("LongList"),getIncompatibleRecord("LongList"), INCOMPATIBLE, READER_FIELD_MISSING_DEFAULT_VALUE, false},

        // missing union branch
        {getUnion(), Schema.create(INT), INCOMPATIBLE, MISSING_UNION_BRANCH, false},

        // Schema reader non valido
        {getInvalidSchema(), Schema.create(NULL), null, null, true},

        // Schema writer non valido
        {Schema.create(NULL), getInvalidSchema(), null, null, true},

    });
  }

  private static Schema getInvalidSchema(){
    Schema schema = mock(Schema.class);
    when(schema.getType()).thenThrow( new RuntimeException("getType() not available"));
    return schema;
  }


  private static Schema getIncompatibleRecord(String name){

    Schema longSchema = Schema.create(Schema.Type.LONG);
    Schema.Field field = new Schema.Field("different", longSchema, null, null);
    List<Schema.Field> recordFields = new ArrayList<>();
    recordFields.add(field);

    Schema schema = Schema.createRecord(name, null, "org.apace.avro", false, recordFields);
    schema.addAlias("LinkedLongs");

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
