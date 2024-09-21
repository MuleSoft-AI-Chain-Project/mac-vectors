package com.mule.mulechain.vectors.internal;

import java.util.Set;
import org.mule.runtime.api.value.Value;
import org.mule.runtime.extension.api.values.ValueBuilder;
import org.mule.runtime.extension.api.values.ValueProvider;
import org.mule.runtime.extension.api.values.ValueResolvingException;

public class MuleChainVectorsParameterEModelNameProvider implements ValueProvider {

  private static final Set<Value> VALUES_FOR = ValueBuilder.getValuesFor(
          "text-embedding-3-small",
          "text-embedding-3-large",
          "text-embedding-ada-002",
          "mistral-embed",
          "all-minilm",
          "nomic-embed-text",
          "tiiuae/falcon-7b-instruct",
          "sentence-transformers/all-MiniLM-L6-v2"
  );

  @Override
  public Set<Value> resolve() throws ValueResolvingException {


    return VALUES_FOR;
  }

}
