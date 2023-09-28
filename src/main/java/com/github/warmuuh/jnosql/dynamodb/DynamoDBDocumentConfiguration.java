package com.github.warmuuh.jnosql.dynamodb;

import java.net.URI;
import org.eclipse.jnosql.communication.Settings;
import org.eclipse.jnosql.communication.document.DocumentConfiguration;
import org.eclipse.jnosql.communication.document.DocumentManagerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

public class DynamoDBDocumentConfiguration implements DocumentConfiguration {

  protected DynamoDbClient getDynamoDB(Settings settings) {
    DynamoDbClientBuilder dynamoDB = DynamoDbClient.builder();

    String awsAccessKey = settings.getOrDefault(DynamoDBDocumentProperties.AWS_ACCESSKEY, "");
    String awsSecretAccess = settings.getOrDefault(DynamoDBDocumentProperties.AWS_SECRET_ACCESS, "");

    settings.get(DynamoDBDocumentProperties.ENDPOINT, String.class).map(URI::create)
        .ifPresent(dynamoDB::endpointOverride);

    settings.get(DynamoDBDocumentProperties.REGION, String.class).map(Region::of)
        .ifPresent(dynamoDB::region);

    settings.get(DynamoDBDocumentProperties.PROFILE, String.class)
        .map(p -> ProfileCredentialsProvider.builder()
            .profileName(p)
            .build())
        .ifPresent(dynamoDB::credentialsProvider);

    boolean accessKey = awsAccessKey != null && !awsAccessKey.equals("");
    boolean secretAccess = awsSecretAccess != null && !awsSecretAccess.equals("");

    if (accessKey && secretAccess) {
      AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(awsAccessKey, awsSecretAccess);
      AwsCredentialsProvider staticCredentialsProvider = StaticCredentialsProvider.create(awsBasicCredentials);
      dynamoDB.credentialsProvider(staticCredentialsProvider);
    }

    return dynamoDB.build();
  }

  @Override
  public DocumentManagerFactory apply(Settings settings) {
    DynamoDbClient dynamoDB = getDynamoDB(settings);
    return new DynamoDbDocumentManagerFactory(dynamoDB, settings);
  }
}
