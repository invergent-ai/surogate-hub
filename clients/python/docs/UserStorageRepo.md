# UserStorageRepo


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** |  | 
**bytes_used** | **int** |  | 

## Example

```python
from surogate_hub_sdk.models.user_storage_repo import UserStorageRepo

# TODO update the JSON string below
json = "{}"
# create an instance of UserStorageRepo from a JSON string
user_storage_repo_instance = UserStorageRepo.from_json(json)
# print the JSON string representation of the object
print(UserStorageRepo.to_json())

# convert the object into a dict
user_storage_repo_dict = user_storage_repo_instance.to_dict()
# create an instance of UserStorageRepo from a dict
user_storage_repo_from_dict = UserStorageRepo.from_dict(user_storage_repo_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


