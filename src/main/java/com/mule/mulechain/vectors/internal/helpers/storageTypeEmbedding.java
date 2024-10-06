package com.mule.mulechain.vectors.internal.helpers;


import java.util.Set;

import org.mule.runtime.api.value.Value;
import org.mule.runtime.extension.api.values.ValueBuilder;
import org.mule.runtime.extension.api.values.ValueProvider;
import org.mule.runtime.extension.api.values.ValueResolvingException;

public class storageTypeEmbedding implements ValueProvider {

  @Override
  public Set<Value> resolve() throws ValueResolvingException {
    // TODO Auto-generated method stub
    return ValueBuilder.getValuesFor("Local", "S3");
  }

}
