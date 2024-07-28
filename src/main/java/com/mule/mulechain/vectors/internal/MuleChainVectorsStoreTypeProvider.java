package com.mule.mulechain.vectors.internal;

import java.util.Set;

import org.mule.runtime.api.value.Value;
import org.mule.runtime.extension.api.values.ValueBuilder;
import org.mule.runtime.extension.api.values.ValueProvider;
import org.mule.runtime.extension.api.values.ValueResolvingException;

public class MuleChainVectorsStoreTypeProvider implements ValueProvider {

  @Override
  public Set<Value> resolve() throws ValueResolvingException {
    // TODO Auto-generated method stub
    return ValueBuilder.getValuesFor("PGVECTOR", "ELASTICSEARCH", "MILVUS", "CHROMA",
                                     "AZURE_SEARCH", "MONGODB", "PINECONE", "WEAVIATE", "NEO4J");
  }

}
