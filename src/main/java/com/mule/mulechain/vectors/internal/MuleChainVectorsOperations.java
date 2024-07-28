package com.mule.mulechain.vectors.internal;

import static org.mule.runtime.extension.api.annotation.param.MediaType.ANY;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.json.JSONObject;
import org.mule.runtime.extension.api.annotation.Alias;
import org.mule.runtime.extension.api.annotation.param.MediaType;
import org.mule.runtime.extension.api.annotation.param.ParameterGroup;
import static java.util.stream.Collectors.joining;
import com.mule.mulechain.vectors.internal.helpers.fileTypeParameters;
import static dev.langchain4j.data.document.loader.FileSystemDocumentLoader.loadDocument;

import dev.langchain4j.data.document.BlankDocumentException;
import dev.langchain4j.data.document.Document;
import dev.langchain4j.data.document.DocumentSplitter;
import dev.langchain4j.data.document.loader.UrlDocumentLoader;
import dev.langchain4j.data.document.parser.TextDocumentParser;
import dev.langchain4j.data.document.parser.apache.tika.ApacheTikaDocumentParser;
import dev.langchain4j.data.document.splitter.DocumentSplitters;
import dev.langchain4j.data.document.transformer.HtmlTextExtractor;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.mistralai.MistralAiEmbeddingModel;
import dev.langchain4j.model.openai.OpenAiEmbeddingModel;
import dev.langchain4j.store.embedding.EmbeddingMatch;
import dev.langchain4j.store.embedding.EmbeddingStore;
import dev.langchain4j.store.embedding.EmbeddingStoreIngestor;
import dev.langchain4j.store.embedding.chroma.ChromaEmbeddingStore;
import dev.langchain4j.store.embedding.elasticsearch.ElasticsearchEmbeddingStore;
import dev.langchain4j.store.embedding.milvus.MilvusEmbeddingStore;
//import dev.langchain4j.store.embedding.neo4j.Neo4jEmbeddingStore;
import dev.langchain4j.store.embedding.pgvector.PgVectorEmbeddingStore;
import dev.langchain4j.store.embedding.pinecone.PineconeEmbeddingStore;
import dev.langchain4j.store.embedding.pinecone.PineconeServerlessIndexConfig;
import dev.langchain4j.store.embedding.weaviate.WeaviateEmbeddingStore;

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


  private EmbeddingStore<TextSegment> createStore(MuleChainVectorsConfiguration configuration, String indexName, Integer dimension) {
    EmbeddingStore<TextSegment> store = null;
    JSONObject config = readConfigFile(configuration.getConfigFilePath());
    JSONObject vectorType;
    String userName;
    String password;
    String vectorHost;
    Integer vectorPort;
    String vectorDatabase; 
    String vectorApiKey;

    String vectorUrl;
    switch (configuration.getVectorDBProviderType()) {
      case "CHROMA":
          vectorType = config.getJSONObject("CHROMA");
          vectorUrl = vectorType.getString("CHROMA_URL");
          store = createChromaStore(vectorUrl, indexName);
        break;
      case "MILVUS":
          vectorType = config.getJSONObject("MILVUS");
          vectorUrl = vectorType.getString("MILVUS_URL");
          store = createMilvusStore(vectorUrl, indexName, dimension);

        break;
      case "PINECONE":
        vectorType = config.getJSONObject("PINECONE");
        vectorApiKey = vectorType.getString("PINECONE_APIKEY");
        String vectorCloud = vectorType.getString("PINECONE_SERVERLESS_CLOUD");
        String vectorCloudRegion = vectorType.getString("PINECONE_SERVERLESS_REGION");
        store = createPineconeStore(vectorApiKey, vectorCloud, vectorCloudRegion, indexName, dimension);

      break;

      case "ELASTICSEARCH":
          vectorType = config.getJSONObject("ELASTICSEARCH");
          vectorUrl = vectorType.getString("ELASTICSEARCH_URL");
          userName = vectorType.getString("ELASTICSEARCH_USER");
          password = vectorType.getString("ELASTICSEARCH_PASSWORD");
          store = createElasticStore(vectorUrl, userName, password, indexName, dimension);
        break;
      case "PGVECTOR":
          vectorType = config.getJSONObject("PGVECTOR");
          vectorHost = vectorType.getString("POSTGRES_HOST");
          vectorPort = vectorType.getInt("POSTGRES_PORT");
          vectorDatabase = vectorType.getString("POSTGRES_DATABASE");
          userName = vectorType.getString("POSTGRES_USER");
          password = vectorType.getString("POSTGRES_PASSWORD");
          store = createPGVectorStore(vectorHost, vectorPort, vectorDatabase, userName, password, indexName, dimension);
        break;

      case "WEAVIATE":
          vectorType = config.getJSONObject("WEAVIATE");
          vectorHost = vectorType.getString("WEAVIATE_HOST");
          String vectorProtocol = vectorType.getString("WEAVIATE_PROTOCOL");
          vectorApiKey = vectorType.getString("WEAVIATE_APIKEY");
          String weaviateIdex = indexName.substring(0, 1).toUpperCase() + indexName.substring(1);
          store = createWeaviateStore(vectorProtocol, vectorHost, vectorApiKey, weaviateIdex);
        break;

      /*case "NEO4J":
          vectorType = config.getJSONObject("NEO4J");
          vectorUrl = vectorType.getString("NEO4J_BOLTURL");
          userName = vectorType.getString("NEO4J_USER");
          password = vectorType.getString("NEO4J_PASSWORD");
          store = createNeo4JStore(vectorUrl, indexName, userName, password, dimension);
        break;*/

        /*case "AZURE_OPENAI":
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
      case "MISTRAL_AI":
          llmType = config.getJSONObject("MISTRAL_AI");
          llmTypeKey = llmType.getString("MISTRAL_AI_API_KEY");
          model = createMistralAIModel(llmTypeKey, modelParams);

        break;
      /*case "OLLAMA":

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

  private EmbeddingModel createMistralAIModel(String llmTypeKey, MuleChainVectorsModelParameters modelParams) {
    return MistralAiEmbeddingModel.builder()
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


  private EmbeddingStore<TextSegment> createMilvusStore(String baseUrl, String collectionName, Integer dimension) {
    return MilvusEmbeddingStore.builder()
      .uri(baseUrl)
      .collectionName(collectionName)
      .dimension(dimension)
      .build();
  }

  private EmbeddingStore<TextSegment> createElasticStore(String baseUrl, String userName, String password, String collectionName, Integer dimension) {
    return ElasticsearchEmbeddingStore.builder()
    .serverUrl(baseUrl)
    .userName(userName)
    .password(password)
    .indexName(collectionName)
    .dimension(dimension)
    .build();
  }

  private EmbeddingStore<TextSegment> createPineconeStore(String apiKey, String cloudProvider, String cloudRegion, String collectionName, Integer dimension) {
    return PineconeEmbeddingStore.builder()
    .apiKey(apiKey)
    .index(collectionName)
    .nameSpace("ns0mc_" + collectionName)
    .createIndex(PineconeServerlessIndexConfig.builder()
            .cloud(cloudProvider)
            .region(cloudRegion)
            .dimension(dimension)
            .build()) 
    .build();
  }

  private EmbeddingStore<TextSegment> createPGVectorStore(String host, Integer port, String database, String userName, String password, String collectionName, Integer dimension) {
    return PgVectorEmbeddingStore.builder()
      .host(host)
      .port(port)
      .database(database)
      .user(userName)
      .password(password)
      .table(collectionName)
      .dimension(dimension)
      .build();
  }

  /*private EmbeddingStore<TextSegment> createNeo4JStore(String boltURL, String userName, String password, String collectionName, Integer dimension) {
    return Neo4jEmbeddingStore.builder()
      .withBasicAuth(boltURL, userName, password)
      .dimension(dimension)
      .databaseName(collectionName)
      .build();
  }*/


  private EmbeddingStore<TextSegment> createWeaviateStore(String protocol, String host, String apiKey, String collectionName) {
    return WeaviateEmbeddingStore.builder()
      .scheme(protocol)
      .host(host)
      // "Default" class is used if not specified. Must start from an uppercase letter!
      .objectClass(collectionName)
      // If true (default), then WeaviateEmbeddingStore will generate a hashed ID based on provided
      // text segment, which avoids duplicated entries in DB. If false, then random ID will be generated.
      .avoidDups(true)
      // Consistency level: ONE, QUORUM (default) or ALL.
      .consistencyLevel("ALL")
      .apiKey(apiKey)
      .build();
  }



  /**
   * Adds Text to Embedding Store
   */
  @MediaType(value = ANY, strict = false)
  @Alias("Embedding-add-text-to-store")
  public String addTextToStore(String storeName, String textToAdd,@Config MuleChainVectorsConfiguration configuration,  @ParameterGroup(name = "Additional Properties") MuleChainVectorsModelParameters modelParams){

    EmbeddingModel embeddingModel = createModel(configuration, modelParams);

    EmbeddingStore<TextSegment> store = createStore(configuration, storeName, embeddingModel.dimension());

    TextSegment textSegment = TextSegment.from(textToAdd);
    Embedding textEmbedding = embeddingModel.embed(textSegment).content();
    store.add(textEmbedding, textSegment); 
    
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("status", "added");
    jsonObject.put("textSegment", textSegment.toString());
    jsonObject.put("textEmbedding", textEmbedding.toString());
    jsonObject.put("storeName", storeName);

    return jsonObject.toString();
  }


   /**
   * Adds Text to Embedding Store
   */
  @MediaType(value = ANY, strict = false)
  @Alias("Embedding-generate-from-text")
  public String generateEmbedding(String textToAdd, @Config MuleChainVectorsConfiguration configuration,  @ParameterGroup(name = "Additional Properties") MuleChainVectorsModelParameters modelParams){

    EmbeddingModel embeddingModel = createModel(configuration, modelParams);

    TextSegment textSegment = TextSegment.from(textToAdd);
    Embedding textEmbedding = embeddingModel.embed(textSegment).content();

    JSONObject jsonObject = new JSONObject();
    jsonObject.put("Segment", textSegment.toString());
    jsonObject.put("Embedding", textEmbedding.toString());
    jsonObject.put("Dimension", textEmbedding.dimension());


    return jsonObject.toString();
  }



  /**
   * Splits a document provided by full path in to a defined set of chucks and overlaps
   */
  @MediaType(value = ANY, strict = false)
  @Alias("Document-split-into-chunks")
  public String documentSplitter(String contextPath, @Config MuleChainVectorsConfiguration configuration,
                                @ParameterGroup(name = "Context") fileTypeParameters fileType, 
                                int maxSegmentSizeInChars, int maxOverlapSizeInChars){



    List<TextSegment> segments;
    DocumentSplitter splitter;
    Document document = null;
    switch (fileType.getFileType()) {
      case "text":
        document = loadDocument(contextPath, new TextDocumentParser());
        splitter = DocumentSplitters.recursive(maxSegmentSizeInChars, maxOverlapSizeInChars);
        segments = splitter.split(document);
        break;
      case "pdf":
        document = loadDocument(contextPath, new ApacheTikaDocumentParser());
        splitter = DocumentSplitters.recursive(maxSegmentSizeInChars, maxOverlapSizeInChars);
        segments = splitter.split(document);
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
        splitter = DocumentSplitters.recursive(maxSegmentSizeInChars, maxOverlapSizeInChars);
        segments = splitter.split(document);
        break;
      default:
        throw new IllegalArgumentException("Unsupported File Type: " + fileType.getFileType());
    }
                                  
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("contextPath", contextPath);
    jsonObject.put("fileType", fileType);
    jsonObject.put("segments", segments.toString());
    
    return jsonObject.toString();
  }

   /**
   * Parses a document by filepath and returns the text
   */
  @MediaType(value = ANY, strict = false)
  @Alias("Document-parser")
  public String documentParser(String contextPath, @Config MuleChainVectorsConfiguration configuration,
                              @ParameterGroup(name = "Context") fileTypeParameters fileType){

    Document document = null;
    switch (fileType.getFileType()) {
      case "text":
        document = loadDocument(contextPath, new TextDocumentParser());
       break;
      case "pdf":
        document = loadDocument(contextPath, new ApacheTikaDocumentParser());
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

        break;
      default:
        throw new IllegalArgumentException("Unsupported File Type: " + fileType.getFileType());
      }
        
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("contextPath", contextPath);
    jsonObject.put("fileType", fileType.getFileType());
    jsonObject.put("documentText",document.text());
    jsonObject.put("metadata",document.metadata());


    return jsonObject.toString();
}


  /**
   * Loads multiple files from a folder into the embedding store. URLs are not supported with this operation.
   */
  @MediaType(value = ANY, strict = false)
  @Alias("Embedding-add-folder-to-store")
  public String addFolderToStore(String storeName, String folderPath, @Config MuleChainVectorsConfiguration configuration,
                                @ParameterGroup(name = "Context") fileTypeParameters fileType, 
                                int maxSegmentSizeInChars, int maxOverlapSizeInChars,
                                @ParameterGroup(name = "Additional Properties") MuleChainVectorsModelParameters modelParams){

    EmbeddingModel embeddingModel = createModel(configuration, modelParams);

    EmbeddingStore<TextSegment> store = createStore(configuration, storeName, embeddingModel.dimension());

    EmbeddingStoreIngestor ingestor = EmbeddingStoreIngestor.builder()
        .documentSplitter(DocumentSplitters.recursive(maxSegmentSizeInChars, maxOverlapSizeInChars))
        .embeddingModel(embeddingModel)
        .embeddingStore(store)
        .build();


    long totalFiles = 0;
    try (Stream<Path> paths = Files.walk(Paths.get(folderPath))) {
      totalFiles = paths.filter(Files::isRegularFile).count();
    } catch (IOException e) {
      e.printStackTrace();
    }

    System.out.println("Total number of files to process: " + totalFiles);
    AtomicInteger fileCounter = new AtomicInteger(0);
    try (Stream<Path> paths = Files.walk(Paths.get(folderPath))) {
      paths.filter(Files::isRegularFile).forEach(file -> {
        int currentFileCounter = fileCounter.incrementAndGet();
        System.out.println("Processing file " + currentFileCounter + ": " + file.getFileName());
        Document document = null;
        try {
          switch (fileType.getFileType()) {
            case "text":
              document = loadDocument(file.toString(), new TextDocumentParser());
              System.out.println("File: " + file.toString());

              ingestor.ingest(document);
              break;
            case "pdf":
              document = loadDocument(file.toString(), new ApacheTikaDocumentParser());
              System.out.println("File: " + file.toString());

              ingestor.ingest(document);
              break;
            default:
              throw new IllegalArgumentException("Unsupported File Type: " + fileType.getFileType());
          }
        } catch (BlankDocumentException e) {
          System.out.println("Skipping file due to BlankDocumentException: " + file.getFileName());
        }
      });
    } catch (IOException e) {
      e.printStackTrace();
    }

    JSONObject jsonObject = new JSONObject();
    jsonObject.put("filesCount", totalFiles);
    jsonObject.put("folderPath", folderPath);
    jsonObject.put("storeName", storeName);
    jsonObject.put("status", "updated");


    return jsonObject.toString();
  }



    /**
   * Add document of type text, pdf and url to embedding store, provide the storeName (Index, Collection, etc).
     * @throws InterruptedException 
   */
  @MediaType(value = ANY, strict = false)
  @Alias("EMBEDDING-add-document-to-store")
  public String addFileEmbedding(String storeName, String contextPath, @Config MuleChainVectorsConfiguration configuration,
                                 @ParameterGroup(name = "Context") fileTypeParameters fileType, 
                                 int maxSegmentSizeInChars, int maxOverlapSizeInChars,
                                 @ParameterGroup(name = "Additional Properties") MuleChainVectorsModelParameters modelParams) {

                                  
    EmbeddingModel embeddingModel = createModel(configuration, modelParams);

    EmbeddingStore<TextSegment> store = createStore(configuration, storeName, embeddingModel.dimension());

    EmbeddingStoreIngestor ingestor = EmbeddingStoreIngestor.builder()
        .documentSplitter(DocumentSplitters.recursive(maxSegmentSizeInChars, maxOverlapSizeInChars))
        .embeddingModel(embeddingModel)
        .embeddingStore(store)
        .build();


    System.out.println("file Type: " + fileType.getFileType());
    
    Document document = null;
    Path filePath = Paths.get(contextPath.toString()); 

    switch (fileType.getFileType()) {
      case "text":
        document = loadDocument(filePath.toString(), new TextDocumentParser());
        ingestor.ingest(document);

        System.out.println("Document contextPath: " + contextPath);

        break;
      case "pdf":
        document = loadDocument(filePath.toString(), new ApacheTikaDocumentParser());
        ingestor.ingest(document);

        System.out.println("Document contextPath: " + contextPath);

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



    JSONObject jsonObject = new JSONObject();
    jsonObject.put("fileType", fileType.getFileType());
    jsonObject.put("filePath", contextPath);
    jsonObject.put("storeName", storeName);
    jsonObject.put("status", "updated");

    return jsonObject.toString();
  }

  /**
   * Query information from embedding store , provide the storeName (Index, Collections, etc.)
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

    EmbeddingStore<TextSegment> store = createStore(configuration, storeName, embeddingModel.dimension());



    Embedding questionEmbedding = embeddingModel.embed(question).content();

    List<EmbeddingMatch<TextSegment>> relevantEmbeddings = store.findRelevant(questionEmbedding, maximumResults, minScore);


    String information = relevantEmbeddings.stream()
        .map(match -> match.embedded().text())
        .collect(joining("\n\n"));



    JSONObject jsonObject = new JSONObject();
    jsonObject.put("maxResults", maxResults);
    jsonObject.put("minScore", minScore);
    jsonObject.put("question", question);
    jsonObject.put("storeName", storeName);
    jsonObject.put("information", information);
    


    
    return jsonObject.toString();
  }



  
}
