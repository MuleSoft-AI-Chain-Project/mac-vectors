package com.mule.mulechain.vectors.internal.storage;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.store.embedding.EmbeddingStoreIngestor;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import com.mule.mulechain.vectors.internal.helpers.fileTypeParameters;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import java.util.concurrent.atomic.AtomicInteger;
import dev.langchain4j.data.document.parser.TextDocumentParser;
import dev.langchain4j.data.document.parser.apache.tika.ApacheTikaDocumentParser;
import dev.langchain4j.data.document.Document;
import dev.langchain4j.store.embedding.EmbeddingStoreIngestor;
import static dev.langchain4j.data.document.loader.FileSystemDocumentLoader.loadDocument;

public class S3FileReader {

    private final S3Client s3Client;
    private final String bucketName;

    public S3FileReader(String bucketName, String awsKey, String awsSecret, String awsRegion) {
        this.bucketName = bucketName;
        AwsBasicCredentials creds = AwsBasicCredentials.create(awsKey, awsSecret); 
        this.s3Client = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(creds))
                .region(Region.of(awsRegion)) // Specify your region
                .build();
    }

    public long readAllFiles(String folderPath, EmbeddingStoreIngestor ingestor, fileTypeParameters fileType) 
    {
        
        int fileCount = countFilesInFolder(folderPath);
        System.out.println("Total number of files in '" + folderPath + "': " + fileCount);

        ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(this.bucketName)
                .prefix(folderPath)
                .build();

        ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);
        long totalFiles = 0;

        AtomicInteger fileCounter = new AtomicInteger(0);
        for (S3Object object : listResponse.contents()) {
            int currentFileCounter = fileCounter.incrementAndGet();
            System.out.println("Ingesting file " + currentFileCounter + ": " + object.key());
            readFile(object.key(), fileType, ingestor);
            totalFiles += 1;
        }
        System.out.println("Total number of files processed: " + totalFiles);
        return totalFiles;
    }

    public void readFile(String key, fileTypeParameters fileType, EmbeddingStoreIngestor ingestor) {
        Document document = null;
        File s3File = new File(key);
        try (InputStream inputStream = s3Client.getObject(GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build())) {

            // Define the local file path in the temporary directory
            String tempDir = System.getProperty("java.io.tmpdir");
            String localFilePath = tempDir + File.separator + s3File.getName(); // Use temp directory
            File localFile = new File(localFilePath);

            switch (fileType.getFileType()) {
                case "text":
                  try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                      FileWriter writer = new FileWriter(localFilePath)) 
                  {
                      String line;
                      while ((line = reader.readLine()) != null) {
                          writer.write(line + "\n");
                      }
                  }
                  document = loadDocument(localFilePath, new TextDocumentParser());
//                  System.out.println("File: " + file.toString());
                  document.metadata().add("file_type", "text");
                  document.metadata().add("file_name", s3File.getName());
                  document.metadata().add("full_path", s3File.getParent());
                  document.metadata().add("absolute_path", s3File.getPath());
                  ingestor.ingest(document);
                  break;
                case "any":
                  try (FileOutputStream outputStream = new FileOutputStream(localFile))
                  {
                    byte[] buffer = new byte[1024];
                    int bytesRead;
    
                    while ((bytesRead = inputStream.read(buffer)) != -1) {
                        outputStream.write(buffer, 0, bytesRead);
                    }
                  }
                  document = loadDocument(localFilePath, new ApacheTikaDocumentParser());
//                  System.out.println("File: " + file.toString());
                  document.metadata().add("file_type", "text");
                  document.metadata().add("file_name", s3File.getName());
                  document.metadata().add("full_path", s3File.getParent());
                  document.metadata().add("absolute_path", s3File.getPath());
                  ingestor.ingest(document);
                  break;
                default:
                  throw new IllegalArgumentException("Unsupported File Type: " + fileType.getFileType());
    
//            System.out.println("Content of " + key + ":");
//            System.out.println(content.toString());
            }
            localFile.delete();
        } catch (IOException e) {
            System.err.println("Error reading file " + key + ": " + e.getMessage());
        }
    }

    private int countFilesInFolder(String folderPath) {
        int count = 0;

        ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(folderPath)
                .build();

        ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);

        count += listResponse.contents().size(); // Count files in the current "folder"

        // Check for pagination
        while (listResponse.isTruncated()) {
            listRequest = listRequest.toBuilder()
                    .continuationToken(listResponse.nextContinuationToken())
                    .build();
            listResponse = s3Client.listObjectsV2(listRequest);
            count += listResponse.contents().size(); // Count files in subsequent pages
        }

        return count;
    }
}
