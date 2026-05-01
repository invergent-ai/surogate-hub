package graveler

import (
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	EntityTypeKey    = "entity"
	EntityTypeCommit = "commit"
	EntityTypeBranch = "branch"
	EntityTypeTag    = "tag"

	EntitySchemaKey    = "schema_name"
	EntitySchemaCommit = "io.invergent.sghub.graveler.CommitData"
	EntitySchemaBranch = "io.invergent.sghub.graveler.BranchData"
	EntitySchemaTag    = "io.invergent.sghub.graveler.TagData"

	EntitySchemaDefinitionKey = "schema_definition"
)

func serializeSchemaDefinition(msg protoreflect.ProtoMessage) (string, error) {
	descriptorProto := protodesc.ToDescriptorProto(msg.ProtoReflect().Descriptor())
	jsonData, err := protojson.Marshal(descriptorProto)
	if err != nil {
		return "", err
	}
	return string(jsonData), nil
}
