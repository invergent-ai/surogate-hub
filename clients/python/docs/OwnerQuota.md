# OwnerQuota


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**quota_bytes** | **int** |  | 

## Example

```python
from surogate_hub_sdk.models.owner_quota import OwnerQuota

# TODO update the JSON string below
json = "{}"
# create an instance of OwnerQuota from a JSON string
owner_quota_instance = OwnerQuota.from_json(json)
# print the JSON string representation of the object
print(OwnerQuota.to_json())

# convert the object into a dict
owner_quota_dict = owner_quota_instance.to_dict()
# create an instance of OwnerQuota from a dict
owner_quota_from_dict = OwnerQuota.from_dict(owner_quota_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


