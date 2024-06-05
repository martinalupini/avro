package org.apache.avro;

public class MockNames extends Schema.Names {

  public MockNames(){}

  @Override
  public void add(Schema schema) {
    throw new RuntimeException("add not available");
  }


  @Override
  public Schema get(String name) {
    throw new RuntimeException("get not available");
  }

}
