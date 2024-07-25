package com.mule.mulechain.vectors.internal;

import org.mule.runtime.extension.api.annotation.Operations;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import org.mule.runtime.extension.api.annotation.values.OfValues;

/**
 * This class represents an extension configuration, values set in this class are commonly used across multiple
 * operations since they represent something core from the extension.
 */
@Operations(MuleChainVectorsOperations.class)
public class MuleChainVectorsConfiguration {

  @Parameter
  @OfValues(MuleChainVectorsEmbeddingModelTypeProvider.class)
  private String embeddingProviderType;

  @Parameter
  @OfValues(MuleChainVectorsStoreTypeProvider.class)
  private String vectorDBProviderType;

  @Parameter
  private String configFilePath;

  public String getEmbeddingProviderType() {
    return embeddingProviderType;
  }

  public String getVectorDBProviderType() {
    return vectorDBProviderType;
  }

  public String getConfigFilePath() {
    return configFilePath;
  }

}
