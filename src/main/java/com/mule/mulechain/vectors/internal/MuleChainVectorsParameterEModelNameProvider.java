package com.mule.mulechain.vectors.internal;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.mule.runtime.api.value.Value;
import org.mule.runtime.extension.api.annotation.param.Config;
import org.mule.runtime.extension.api.values.ValueBuilder;
import org.mule.runtime.extension.api.values.ValueProvider;
import org.mule.runtime.extension.api.values.ValueResolvingException;

public class MuleChainVectorsParameterEModelNameProvider implements ValueProvider {

  @Config
  private MuleChainVectorsConfiguration configuration;

  private static final Set<Value> VALUES_FOR_OPENAI = ValueBuilder.getValuesFor(
          "text-embedding-3-small",
          "text-embedding-3-large",
          "text-embedding-ada-002"
  );

  private static final Set<Value> VALUES_FOR_MISTRAL_AI = ValueBuilder.getValuesFor(
          "mistral-embed"
  );

  private static final Set<Value> VALUES_FOR_NOMIC = ValueBuilder.getValuesFor(
          "nomic-embed-text"
  );

  private static final Set<Value> VALUES_FOR_HUGGING_FACE = ValueBuilder.getValuesFor(
            "tiiuae/falcon-7b-instruct",
            "sentence-transformers/all-MiniLM-L6-v2"
  );

  @Override
  public Set<Value> resolve() throws ValueResolvingException {
    String embeddingProviderType = configuration.getEmbeddingProviderType();
    switch (embeddingProviderType) {
      case "OPENAI":
        return VALUES_FOR_OPENAI;
      case "MISTRAL_AI":
        return VALUES_FOR_MISTRAL_AI;
      case "NOMIC":
        return VALUES_FOR_NOMIC;
      case "HUGGING_FACE":
        return VALUES_FOR_HUGGING_FACE;
      default:
        return Collections.emptySet();
    }
  }

}
