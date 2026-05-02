# ObjectUploadMode


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**upload_mode** | **str** | Server-selected upload mode for a regular object upload. \&quot;regular\&quot; means the client should use POST /objects. \&quot;xet\&quot; means the client may upload through XET and link an xet:// physical address.  | 

## Example

```python
from surogate_hub_sdk.models.object_upload_mode import ObjectUploadMode

# TODO update the JSON string below
json = "{}"
# create an instance of ObjectUploadMode from a JSON string
object_upload_mode_instance = ObjectUploadMode.from_json(json)
# print the JSON string representation of the object
print(ObjectUploadMode.to_json())

# convert the object into a dict
object_upload_mode_dict = object_upload_mode_instance.to_dict()
# create an instance of ObjectUploadMode from a dict
object_upload_mode_from_dict = ObjectUploadMode.from_dict(object_upload_mode_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


