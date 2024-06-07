package org.apache.avro;

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
    import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class TestSchemaCompatibilityTypeMismatch {

  private Schema reader;
  private Schema writer;
  private SchemaCompatibility.SchemaCompatibilityType expectedCompatibilityType;
  private SchemaCompatibility.SchemaIncompatibilityType expectedIncompatibilityType;
  private boolean expectedException;

  public TestSchemaCompatibilityTypeMismatch(Schema readerSchema, Schema writerSchema, SchemaCompatibility.SchemaCompatibilityType expectedCompatibilityType, SchemaCompatibility.SchemaIncompatibilityType expectedIncompatibilityType, boolean expectedException) {
    this.reader = readerSchema;
    this.writer = writerSchema;
    this.expectedCompatibilityType = expectedCompatibilityType;
    this.expectedIncompatibilityType = expectedIncompatibilityType;
    this.expectedException = expectedException;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> getParameters(){
    return Arrays.asList(new Object[][]{

        // Type mismatch
        {Schema.create(STRING), Schema.create(INT), INCOMPATIBLE, TYPE_MISMATCH, false},
        {Schema.create(INT), getRecord("LongList"), INCOMPATIBLE, TYPE_MISMATCH, false},
        {getMap(), getRecord("LongList"), INCOMPATIBLE, TYPE_MISMATCH, false},
        {Schema.create(BOOLEAN), Schema.create(LONG), INCOMPATIBLE, TYPE_MISMATCH, false},
        {Schema.create(NULL), Schema.create(BYTES), INCOMPATIBLE, TYPE_MISMATCH, false},
        {Schema.create(BYTES), Schema.create(FLOAT), INCOMPATIBLE, TYPE_MISMATCH, false},
        {Schema.create(FLOAT), getRecord("LongList"), INCOMPATIBLE, TYPE_MISMATCH, false},
        {Schema.create(DOUBLE), getRecord("LongList"), INCOMPATIBLE, TYPE_MISMATCH, false},
        {Schema.create(LONG), getRecord("LongList"), INCOMPATIBLE, TYPE_MISMATCH, false},
        {getRecord("Record"), Schema.create(DOUBLE), INCOMPATIBLE, TYPE_MISMATCH, false},
        {getArray(), Schema.create(BOOLEAN), INCOMPATIBLE, TYPE_MISMATCH, false},
        {getFixed("md5", 16), getRecord("LongList"), INCOMPATIBLE, TYPE_MISMATCH, false},
        {getEnum("Suit"), Schema.create(DOUBLE), INCOMPATIBLE, TYPE_MISMATCH, false},


    });
  }

  private static Schema getInvalidSchema(){
    Schema schema = mock(Schema.class);
    when(schema.getType()).thenReturn(null);
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
