# UserStorage


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**user** | **str** |  | 
**bytes_used** | **int** |  | 
**quota_bytes** | **int** |  | [optional] 
**bytes_remaining** | **int** |  | [optional] 
**repositories** | [**List[UserStorageRepo]**](UserStorageRepo.md) |  | 
**last_reconciled_at** | **datetime** |  | [optional] 
**is_estimate** | **bool** |  | 

## Example

```python
from surogate_hub_sdk.models.user_storage import UserStorage

# TODO update the JSON string below
json = "{}"
# create an instance of UserStorage from a JSON string
user_storage_instance = UserStorage.from_json(json)
# print the JSON string representation of the object
print(UserStorage.to_json())

# convert the object into a dict
user_storage_dict = user_storage_instance.to_dict()
# create an instance of UserStorage from a dict
user_storage_from_dict = UserStorage.from_dict(user_storage_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


