# surogate_hub_sdk.StorageApi

All URIs are relative to */api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**delete_owner_quota**](StorageApi.md#delete_owner_quota) | **DELETE** /storage/owners/{owner}/quota | clear an owner namespace&#39;s storage quota
[**get_owner_storage**](StorageApi.md#get_owner_storage) | **GET** /storage/owners/{owner} | get an owner namespace&#39;s storage usage and quota
[**set_owner_quota**](StorageApi.md#set_owner_quota) | **PUT** /storage/owners/{owner}/quota | set an owner namespace&#39;s storage quota


# **delete_owner_quota**
> delete_owner_quota(owner)

clear an owner namespace's storage quota

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):

```python
import surogate_hub_sdk
from surogate_hub_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = surogate_hub_sdk.Configuration(
    host = "/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = surogate_hub_sdk.Configuration(
    username = os.environ["USERNAME"],
    password = os.environ["PASSWORD"]
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Configure API key authorization: saml_auth
configuration.api_key['saml_auth'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['saml_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = surogate_hub_sdk.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with surogate_hub_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = surogate_hub_sdk.StorageApi(api_client)
    owner = 'owner_example' # str | The repository owner namespace — same meaning as in /storage/owners/{owner}. 

    try:
        # clear an owner namespace's storage quota
        api_instance.delete_owner_quota(owner)
    except Exception as e:
        print("Exception when calling StorageApi->delete_owner_quota: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **owner** | **str**| The repository owner namespace — same meaning as in /storage/owners/{owner}.  | 

### Return type

void (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | quota cleared |  -  |
**401** | Unauthorized |  -  |
**503** | storage usage tracking is disabled |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_owner_storage**
> OwnerStorage get_owner_storage(owner)

get an owner namespace's storage usage and quota

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):

```python
import surogate_hub_sdk
from surogate_hub_sdk.models.owner_storage import OwnerStorage
from surogate_hub_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = surogate_hub_sdk.Configuration(
    host = "/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = surogate_hub_sdk.Configuration(
    username = os.environ["USERNAME"],
    password = os.environ["PASSWORD"]
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Configure API key authorization: saml_auth
configuration.api_key['saml_auth'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['saml_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = surogate_hub_sdk.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with surogate_hub_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = surogate_hub_sdk.StorageApi(api_client)
    owner = 'owner_example' # str | The repository owner namespace — the first path segment of every repo id (e.g. `p-39264d5a` for `p-39264d5a/skill-doc-extract`). Not necessarily a registered hub auth user; can be any synthetic project/workspace id. 

    try:
        # get an owner namespace's storage usage and quota
        api_response = api_instance.get_owner_storage(owner)
        print("The response of StorageApi->get_owner_storage:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling StorageApi->get_owner_storage: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **owner** | **str**| The repository owner namespace — the first path segment of every repo id (e.g. &#x60;p-39264d5a&#x60; for &#x60;p-39264d5a/skill-doc-extract&#x60;). Not necessarily a registered hub auth user; can be any synthetic project/workspace id.  | 

### Return type

[**OwnerStorage**](OwnerStorage.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | storage usage |  -  |
**401** | Unauthorized |  -  |
**503** | storage usage tracking is disabled |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **set_owner_quota**
> set_owner_quota(owner, owner_quota)

set an owner namespace's storage quota

### Example

* Basic Authentication (basic_auth):
* Api Key Authentication (cookie_auth):
* Api Key Authentication (oidc_auth):
* Api Key Authentication (saml_auth):
* Bearer (JWT) Authentication (jwt_token):

```python
import surogate_hub_sdk
from surogate_hub_sdk.models.owner_quota import OwnerQuota
from surogate_hub_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = surogate_hub_sdk.Configuration(
    host = "/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: basic_auth
configuration = surogate_hub_sdk.Configuration(
    username = os.environ["USERNAME"],
    password = os.environ["PASSWORD"]
)

# Configure API key authorization: cookie_auth
configuration.api_key['cookie_auth'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['cookie_auth'] = 'Bearer'

# Configure API key authorization: oidc_auth
configuration.api_key['oidc_auth'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['oidc_auth'] = 'Bearer'

# Configure API key authorization: saml_auth
configuration.api_key['saml_auth'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['saml_auth'] = 'Bearer'

# Configure Bearer authorization (JWT): jwt_token
configuration = surogate_hub_sdk.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with surogate_hub_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = surogate_hub_sdk.StorageApi(api_client)
    owner = 'owner_example' # str | The repository owner namespace — same meaning as in /storage/owners/{owner}. 
    owner_quota = surogate_hub_sdk.OwnerQuota() # OwnerQuota | 

    try:
        # set an owner namespace's storage quota
        api_instance.set_owner_quota(owner, owner_quota)
    except Exception as e:
        print("Exception when calling StorageApi->set_owner_quota: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **owner** | **str**| The repository owner namespace — same meaning as in /storage/owners/{owner}.  | 
 **owner_quota** | [**OwnerQuota**](OwnerQuota.md)|  | 

### Return type

void (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [oidc_auth](../README.md#oidc_auth), [saml_auth](../README.md#saml_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | quota set |  -  |
**400** | Bad Request |  -  |
**401** | Unauthorized |  -  |
**503** | storage usage tracking is disabled |  -  |
**0** | Internal Server Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

