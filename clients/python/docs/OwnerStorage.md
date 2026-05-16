# OwnerStorage


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**owner** | **str** | The repository owner namespace — the first path segment of every repo id this payload aggregates over. Not necessarily a registered hub auth user.  | 
**bytes_used** | **int** |  | 
**quota_bytes** | **int** |  | [optional] 
**bytes_remaining** | **int** |  | [optional] 
**repositories** | [**List[OwnerStorageRepo]**](OwnerStorageRepo.md) |  | 
**last_reconciled_at** | **datetime** |  | [optional] 
**is_estimate** | **bool** |  | 

## Example

```python
from surogate_hub_sdk.models.owner_storage import OwnerStorage

# TODO update the JSON string below
json = "{}"
# create an instance of OwnerStorage from a JSON string
owner_storage_instance = OwnerStorage.from_json(json)
# print the JSON string representation of the object
print(OwnerStorage.to_json())

# convert the object into a dict
owner_storage_dict = owner_storage_instance.to_dict()
# create an instance of OwnerStorage from a dict
owner_storage_from_dict = OwnerStorage.from_dict(owner_storage_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


