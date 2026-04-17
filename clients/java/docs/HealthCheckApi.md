# HealthCheckApi

All URIs are relative to */api/v1*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**healthCheck**](HealthCheckApi.md#healthCheck) | **GET** /healthcheck |  |


<a id="healthCheck"></a>
# **healthCheck**
> healthCheck().execute();



check that the API server is up and running

### Example
```java
// Import classes:
import org.surogate.hub.clients.sdk.ApiClient;
import org.surogate.hub.clients.sdk.ApiException;
import org.surogate.hub.clients.sdk.Configuration;
import org.surogate.hub.clients.sdk.models.*;
import org.surogate.hub.clients.sdk.HealthCheckApi;

public class Example {
  public static void main(String[] args) {
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("/api/v1");

    HealthCheckApi apiInstance = new HealthCheckApi(defaultClient);
    try {
      apiInstance.healthCheck()
            .execute();
    } catch (ApiException e) {
      System.err.println("Exception when calling HealthCheckApi#healthCheck");
      System.err.println("Status code: " + e.getCode());
      System.err.println("Reason: " + e.getResponseBody());
      System.err.println("Response headers: " + e.getResponseHeaders());
      e.printStackTrace();
    }
  }
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **204** | NoContent |  -  |

