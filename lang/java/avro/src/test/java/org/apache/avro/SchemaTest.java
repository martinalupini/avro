package org.apache.avro;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.avro.Schema.Type.STRING;

public class SchemaTest {

  @Test
  public void testSchema() {
    Schema schema = Schema.create(STRING);

    Assert.assertEquals(schema, schema);
  }
}
