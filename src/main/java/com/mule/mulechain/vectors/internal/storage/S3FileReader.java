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
import dev.langchain4j.data.document.loader.amazon.s3.AmazonS3DocumentLoader;
import dev.langchain4j.data.document.loader.amazon.s3.AwsCredentials;
import dev.langchain4j.data.document.DocumentParser;
import java.util.List;

import com.mule.mulechain.vectors.internal.helpers.fileTypeParameters;
import java.util.concurrent.atomic.AtomicInteger;
import dev.langchain4j.data.document.parser.TextDocumentParser;
import dev.langchain4j.data.document.parser.apache.tika.ApacheTikaDocumentParser;
import dev.langchain4j.data.document.Document;
import dev.langchain4j.store.embedding.EmbeddingStoreIngestor;
import static dev.langchain4j.data.document.loader.FileSystemDocumentLoader.loadDocument;

public class S3FileReader {

    private final String bucketName;
    private final AmazonS3DocumentLoader loader;

    public S3FileReader(String bucketName, String awsKey, String awsSecret, String awsRegion) {
        this.bucketName = bucketName;
        AwsCredentials creds = new AwsCredentials(awsKey, awsSecret);
        this.loader = AmazonS3DocumentLoader.builder()
                    .region(awsRegion)
                    .awsCredentials(creds)
                    .build();
    }

    public long readAllFiles(String folderPath, EmbeddingStoreIngestor ingestor, fileTypeParameters fileType) 
    {
        DocumentParser parser = null;
        switch (fileType.getFileType()){
            case "text":
                parser = new TextDocumentParser();
                break;
            case "any":
                parser = new ApacheTikaDocumentParser();
                break;
            default:
                throw new IllegalArgumentException("Unsupported File Type: " + fileType.getFileType());
        }

        List<Document> documents = loader.loadDocuments(bucketName, folderPath, parser);
        int fileCount = documents.size();
        System.out.println("Total number of files in '" + folderPath + "': " + fileCount);

        long totalFiles = 0;
        for (Document document : documents) {
            ingestor.ingest(document);
            totalFiles += 1;
        }
        System.out.println("Total number of files processed: " + totalFiles);
        return totalFiles;
    }

    public void readFile(String key, fileTypeParameters fileType, EmbeddingStoreIngestor ingestor) {
        DocumentParser parser = null;
        switch (fileType.getFileType()){
            case "text":
                parser = new TextDocumentParser();
                break;
            case "any":
                parser = new ApacheTikaDocumentParser();
                break;
            default:
                throw new IllegalArgumentException("Unsupported File Type: " + fileType.getFileType());
        }
        Document document = loader.loadDocument(bucketName, key, parser);
        ingestor.ingest(document);
    }
}
