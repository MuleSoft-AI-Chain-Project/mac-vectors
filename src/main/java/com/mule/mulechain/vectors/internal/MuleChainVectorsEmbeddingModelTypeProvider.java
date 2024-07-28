package com.mule.mulechain.vectors.internal;

import java.util.Set;

import org.mule.runtime.api.value.Value;
import org.mule.runtime.extension.api.values.ValueBuilder;
import org.mule.runtime.extension.api.values.ValueProvider;
import org.mule.runtime.extension.api.values.ValueResolvingException;

public class MuleChainVectorsEmbeddingModelTypeProvider implements ValueProvider {

  @Override
  public Set<Value> resolve() throws ValueResolvingException {
    return ValueBuilder.getValuesFor("OPENAI", "MISTRAL_AI", "NOMIC", "OLLAMA", "COHERE",
                                     "AZURE_OPENAI", "HUGGING_FACE");
  }

}
