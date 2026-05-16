# UserQuota


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**quota_bytes** | **int** |  | 

## Example

```python
from surogate_hub_sdk.models.user_quota import UserQuota

# TODO update the JSON string below
json = "{}"
# create an instance of UserQuota from a JSON string
user_quota_instance = UserQuota.from_json(json)
# print the JSON string representation of the object
print(UserQuota.to_json())

# convert the object into a dict
user_quota_dict = user_quota_instance.to_dict()
# create an instance of UserQuota from a dict
user_quota_from_dict = UserQuota.from_dict(user_quota_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


