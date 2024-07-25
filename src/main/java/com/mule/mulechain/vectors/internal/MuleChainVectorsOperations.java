package com.mule.mulechain.vectors.internal;

import static org.mule.runtime.extension.api.annotation.param.MediaType.ANY;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.json.JSONObject;
import org.mule.runtime.extension.api.annotation.Alias;
import org.mule.runtime.extension.api.annotation.param.MediaType;
import org.mule.runtime.extension.api.annotation.param.ParameterGroup;
import static java.util.stream.Collectors.joining;
import com.mule.mulechain.vectors.internal.helpers.fileTypeParameters;
import static dev.langchain4j.data.document.loader.FileSystemDocumentLoader.loadDocument;

import dev.langchain4j.data.document.Document;
import dev.langchain4j.data.document.loader.UrlDocumentLoader;
import dev.langchain4j.data.document.parser.TextDocumentParser;
import dev.langchain4j.data.document.parser.apache.tika.ApacheTikaDocumentParser;
import dev.langchain4j.data.document.splitter.DocumentSplitters;
import dev.langchain4j.data.document.transformer.HtmlTextExtractor;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.openai.OpenAiEmbeddingModel;
import dev.langchain4j.store.embedding.EmbeddingMatch;
import dev.langchain4j.store.embedding.EmbeddingStore;
import dev.langchain4j.store.embedding.EmbeddingStoreIngestor;
import dev.langchain4j.store.embedding.chroma.ChromaEmbeddingStore;
import org.mule.runtime.extension.api.annotation.param.Config;


/**
 * This class is a container for operations, every public method in this class will be taken as an extension operation.
 */
public class MuleChainVectorsOperations {


    private static JSONObject readConfigFile(String filePath) {
    Path path = Paths.get(filePath);
    if (Files.exists(path)) {
      try {
        String content = new String(Files.readAllBytes(path));
        return new JSONObject(content);
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      //System.out.println("File does not exist: " + filePath);
    }
    return null;
  }


  private EmbeddingStore<TextSegment> createStore(MuleChainVectorsConfiguration configuration, String indexName) {
    EmbeddingStore<TextSegment> store = null;
    JSONObject config = readConfigFile(configuration.getConfigFilePath());
    JSONObject vectorType;

    String vectorUrl;
    switch (configuration.getVectorDBProviderType()) {
      case "CHROMA":
          vectorType = config.getJSONObject("CHROMA");
          vectorUrl = vectorType.getString("CHROMA_URL");
          store = createChromaStore(vectorUrl, indexName);
        break;
      /* case "MISTRAL_AI":
          llmType = config.getJSONObject("MISTRAL_AI");
          llmTypeKey = llmType.getString("MISTRAL_AI_API_KEY");
          model = createMistralAiModel(llmTypeKey, modelParams);

        break;
      case "OLLAMA":

          llmType = config.getJSONObject("OLLAMA");
          String llmTypeUrl = llmType.getString("OLLAMA_BASE_URL");
          model = createOllamaChatModel(llmTypeUrl, modelParams);

        break;
      case "COHERE":
          llmType = config.getJSONObject("COHERE");
          llmTypeKey = llmType.getString("COHERE_API_KEY");
          model = createCohereModel(llmTypeKey, modelParams);
        break;
      case "AZURE_OPENAI":
          llmType = config.getJSONObject("AZURE_OPENAI");
          llmTypeKey = llmType.getString("AZURE_OPENAI_KEY");
          String llmEndpoint = llmType.getString("AZURE_OPENAI_ENDPOINT");
          String llmDeploymentName = llmType.getString("AZURE_OPENAI_DEPLOYMENT_NAME");
          model = createAzureOpenAiModel(llmTypeKey, llmEndpoint, llmDeploymentName, modelParams);
        break; */
      default:
        throw new IllegalArgumentException("Unsupported VectorDB type: " + configuration.getEmbeddingProviderType());
    }

    return store;
  }

  private EmbeddingModel createModel(MuleChainVectorsConfiguration configuration, MuleChainVectorsModelParameters modelParams) {
    EmbeddingModel model = null;
    JSONObject config = readConfigFile(configuration.getConfigFilePath());
    JSONObject llmType;
    String llmTypeKey;

    switch (configuration.getEmbeddingProviderType()) {
      case "OPENAI":
          llmType = config.getJSONObject("OPENAI");
          llmTypeKey = llmType.getString("OPENAI_API_KEY");
          model = createOpenAiModel(llmTypeKey, modelParams);
        break;
      /* case "MISTRAL_AI":
          llmType = config.getJSONObject("MISTRAL_AI");
          llmTypeKey = llmType.getString("MISTRAL_AI_API_KEY");
          model = createMistralAiModel(llmTypeKey, modelParams);

        break;
      case "OLLAMA":

          llmType = config.getJSONObject("OLLAMA");
          String llmTypeUrl = llmType.getString("OLLAMA_BASE_URL");
          model = createOllamaChatModel(llmTypeUrl, modelParams);

        break;
      case "COHERE":
          llmType = config.getJSONObject("COHERE");
          llmTypeKey = llmType.getString("COHERE_API_KEY");
          model = createCohereModel(llmTypeKey, modelParams);
        break;
      case "AZURE_OPENAI":
          llmType = config.getJSONObject("AZURE_OPENAI");
          llmTypeKey = llmType.getString("AZURE_OPENAI_KEY");
          String llmEndpoint = llmType.getString("AZURE_OPENAI_ENDPOINT");
          String llmDeploymentName = llmType.getString("AZURE_OPENAI_DEPLOYMENT_NAME");
          model = createAzureOpenAiModel(llmTypeKey, llmEndpoint, llmDeploymentName, modelParams);
        break; */
      default:
        throw new IllegalArgumentException("Unsupported Embedding Model: " + configuration.getEmbeddingProviderType());
    }
    return model;
  }

  private EmbeddingModel createOpenAiModel(String llmTypeKey, MuleChainVectorsModelParameters modelParams) {
      return OpenAiEmbeddingModel.builder()
        .apiKey(llmTypeKey)
        .modelName(modelParams.getModelName())
        .build();
    }

  private EmbeddingStore<TextSegment> createChromaStore(String baseUrl, String collectionName) {
    return ChromaEmbeddingStore.builder()
      .baseUrl(baseUrl)
      .collectionName(collectionName)
      .build();
  }



  /**
   * Example of an operation that uses the configuration and a connection instance to perform some action.
   */
  @MediaType(value = ANY, strict = false)
  @Alias("Embedding-adhoc-RAG")
  public String embeddingNew(@Config MuleChainVectorsConfiguration configuration){
    return "";
  }


    /**
   * Add document of type text, pdf and url to embedding store (in-memory), which is exported to the defined storeName (full path)
   */
  @MediaType(value = ANY, strict = false)
  @Alias("EMBEDDING-add-document-to-store")
  public String addFileEmbedding(String storeName, String contextPath, @Config MuleChainVectorsConfiguration configuration,
                                 @ParameterGroup(name = "Context") fileTypeParameters fileType, 
                                 int maxSegmentSizeInChars, int maxOverlapSizeInChars,
                                 @ParameterGroup(name = "Additional Properties") MuleChainVectorsModelParameters modelParams) {

                                  
    EmbeddingModel embeddingModel = createModel(configuration, modelParams);

    EmbeddingStore<TextSegment> store = createStore(configuration, storeName);

    EmbeddingStoreIngestor ingestor = EmbeddingStoreIngestor.builder()
        .documentSplitter(DocumentSplitters.recursive(maxSegmentSizeInChars, maxOverlapSizeInChars))
        .embeddingModel(embeddingModel)
        .embeddingStore(store)
        .build();


    Document document = null;
    switch (fileType.getFileType()) {
      case "text":
        document = loadDocument(contextPath, new TextDocumentParser());
        ingestor.ingest(document);
        break;
      case "pdf":
        document = loadDocument(contextPath, new ApacheTikaDocumentParser());
        ingestor.ingest(document);
        break;
      case "url":
        URL url = null;
        try {
          url = new URL(contextPath);
        } catch (MalformedURLException e) {
          e.printStackTrace();
        }

        Document htmlDocument = UrlDocumentLoader.load(url, new TextDocumentParser());
        HtmlTextExtractor transformer = new HtmlTextExtractor(null, null, true);
        document = transformer.transform(htmlDocument);
        document.metadata().add("url", contextPath);
        ingestor.ingest(document);
        break;
      default:
        throw new IllegalArgumentException("Unsupported File Type: " + fileType.getFileType());
    }


    return "Embedding-store updated.";
  }

  /**
   * Query information from embedding store (in-Memory), which is imported from the storeName (full path)
   */
  @MediaType(value = ANY, strict = false)
  @Alias("EMBEDDING-query-from-store")
  public String queryFromEmbedding(String storeName, String question, Number maxResults, Double minScore, 
                                  @Config MuleChainVectorsConfiguration configuration,
                                  @ParameterGroup(name = "Additional Properties") MuleChainVectorsModelParameters modelParams) {
    int maximumResults = (int) maxResults;
    if (minScore == null || minScore == 0) {
      minScore = 0.7;
    }


    EmbeddingModel embeddingModel = createModel(configuration, modelParams);

    EmbeddingStore<TextSegment> store = createStore(configuration, storeName);



    Embedding questionEmbedding = embeddingModel.embed(question).content();

    List<EmbeddingMatch<TextSegment>> relevantEmbeddings = store.findRelevant(questionEmbedding, maximumResults, minScore);

    String information = relevantEmbeddings.stream()
        .map(match -> match.embedded().text())
        .collect(joining("\n\n"));


    return information;
  }



  
}
