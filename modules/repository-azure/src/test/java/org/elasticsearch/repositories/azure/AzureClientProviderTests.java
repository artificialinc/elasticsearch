/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.azure;

import com.azure.storage.common.policy.RequestRetryOptions;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.net.URL;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class AzureClientProviderTests extends ESTestCase {
    private static final BiConsumer<String, URL> EMPTY_CONSUMER = (method, url) -> {};

    private ThreadPool threadPool;
    private AzureClientProvider azureClientProvider;

    private static final String CLIENT_ID = "my-client-id";
    private static final String TENANT_ID = "my-tenant-id";


    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(
            getTestName(),
            AzureRepositoryPlugin.executorBuilder(),
            AzureRepositoryPlugin.nettyEventLoopExecutorBuilder(Settings.EMPTY)
        );
    }

    @After
    public void tearDownThreadPool() {
        azureClientProvider.close();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testCanCreateAClientWithSecondaryLocation() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("azure.client.azure1.account", "myaccount1");
        secureSettings.setString("azure.client.azure1.key", encodeKey("mykey1"));

        final String endpoint;
        if (randomBoolean()) {
            endpoint = "ignored;BlobEndpoint=https://myaccount1.blob.core.windows.net;"
                + "BlobSecondaryEndpoint=https://myaccount1-secondary.blob.core.windows.net";
        } else {
            endpoint = "core.windows.net";
        }

        final Settings settings = Settings.builder()
            .setSecureSettings(secureSettings)
            .put("azure.client.azure1.endpoint_suffix", endpoint)
            .build();

        Map<String, AzureStorageSettings> clientSettings = AzureStorageSettings.load(settings);
        AzureStorageSettings storageSettings = clientSettings.get("azure1");
        assertNotNull(storageSettings);

        LocationMode locationMode = LocationMode.SECONDARY_ONLY;
        RequestRetryOptions requestRetryOptions = new RequestRetryOptions();

        Environment environment = Mockito.mock(Environment.class);
        Map<String, String> environmentVariables = Map.of();
        azureClientProvider = AzureClientProvider.create(threadPool, Settings.EMPTY, environment, environmentVariables::get);
        azureClientProvider.createClient(storageSettings, locationMode, requestRetryOptions, null, EMPTY_CONSUMER);
    }

    public void testCanCreateAClientWithWorkloadIdentityNoClientID() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("azure.client.azure1.account", "myaccount1");

        final String endpoint;
        if (randomBoolean()) {
            endpoint = "ignored;BlobEndpoint=https://myaccount1.blob.core.windows.net;"
                + "BlobSecondaryEndpoint=https://myaccount1-secondary.blob.core.windows.net";
        } else {
            endpoint = "core.windows.net";
        }

        final Settings settings = Settings.builder()
            .setSecureSettings(secureSettings)
            .put("azure.client.azure1.endpoint_suffix", endpoint)
            .build();

        Map<String, AzureStorageSettings> clientSettings = AzureStorageSettings.load(settings);
        AzureStorageSettings storageSettings = clientSettings.get("azure1");
        assertNotNull(storageSettings);

        LocationMode locationMode = LocationMode.SECONDARY_ONLY;
        RequestRetryOptions requestRetryOptions = new RequestRetryOptions();

        Environment environment = Mockito.mock(Environment.class);
        Map<String, String> environmentVariables = Map.of();
        azureClientProvider = AzureClientProvider.create(threadPool, Settings.EMPTY, environment, environmentVariables::get);
        IllegalStateException thrown = expectThrows(java.lang.IllegalStateException.class, () -> {
            azureClientProvider.createClient(storageSettings, locationMode, requestRetryOptions, null, EMPTY_CONSUMER);
        });
        assertTrue(thrown.getMessage().contains("AZURE_CLIENT_ID"));
    }

    public void testCanCreateAClientWithWorkloadIdentitySuccess() throws Exception {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("azure.client.azure1.account", "myaccount1");

        final String endpoint;
        if (randomBoolean()) {
            endpoint = "ignored;BlobEndpoint=https://myaccount1.blob.core.windows.net;"
                + "BlobSecondaryEndpoint=https://myaccount1-secondary.blob.core.windows.net";
        } else {
            endpoint = "core.windows.net";
        }

        final Settings settings = Settings.builder()
            .setSecureSettings(secureSettings)
            .put("azure.client.azure1.endpoint_suffix", endpoint)
            .build();

        Map<String, AzureStorageSettings> clientSettings = AzureStorageSettings.load(settings);
        AzureStorageSettings storageSettings = clientSettings.get("azure1");
        assertNotNull(storageSettings);
        assertTrue(storageSettings.useWorkloadIdentityCredential());

        LocationMode locationMode = LocationMode.SECONDARY_ONLY;
        RequestRetryOptions requestRetryOptions = new RequestRetryOptions();

        Path configDirectory = Files.createTempDirectory("federated-token-test");
        Files.createDirectory(configDirectory.resolve("repository-azure"));
        Files.writeString(configDirectory.resolve("repository-azure/azure-federated-token-file"), "YXdzLXdlYi1pZGVudGl0eS10b2tlbi1maWxl");
        Environment environment = Mockito.mock(Environment.class);
        Mockito.when(environment.configFile()).thenReturn(configDirectory);

        // No region is set, but the SDK shouldn't fail because of that
        Map<String, String> environmentVariables = Map.of(
            "AZURE_FEDERATED_TOKEN_FILE",
            "/var/run/secrets/azure/tokens/azure-identity-token",
            "AZURE_CLIENT_ID",
            CLIENT_ID,
            "AZURE_TENANT_ID",
            TENANT_ID
        );

        azureClientProvider = AzureClientProvider.create(threadPool, Settings.EMPTY, environment, environmentVariables::get);
        azureClientProvider.createClient(storageSettings, locationMode, requestRetryOptions, null, EMPTY_CONSUMER);
    }

    public void testCanNotCreateAClientWithSecondaryLocationWithoutAProperEndpoint() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("azure.client.azure1.account", "myaccount1");
        secureSettings.setString("azure.client.azure1.key", encodeKey("mykey1"));

        final String endpoint = "ignored;BlobEndpoint=https://myaccount1.blob.core.windows.net";

        final Settings settings = Settings.builder()
            .setSecureSettings(secureSettings)
            .put("azure.client.azure1.endpoint_suffix", endpoint)
            .build();

        Map<String, AzureStorageSettings> clientSettings = AzureStorageSettings.load(settings);
        AzureStorageSettings storageSettings = clientSettings.get("azure1");
        assertNotNull(storageSettings);

        LocationMode locationMode = LocationMode.SECONDARY_ONLY;
        RequestRetryOptions requestRetryOptions = new RequestRetryOptions();

        Environment environment = Mockito.mock(Environment.class);
        Map<String, String> environmentVariables = Map.of();
        azureClientProvider = AzureClientProvider.create(threadPool, Settings.EMPTY, environment, environmentVariables::get);
        expectThrows(IllegalArgumentException.class, () -> {
            azureClientProvider.createClient(storageSettings, locationMode, requestRetryOptions, null, EMPTY_CONSUMER);
        });
    }

    private static String encodeKey(final String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
}
