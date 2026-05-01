package model

import (
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/invergent-ai/surogate-hub/pkg/kv"
	"github.com/invergent-ai/surogate-hub/pkg/permissions"
)

var ErrValidationError = errors.New("validation error")

func ValidateAuthEntityID(name string) error {
	if len(name) == 0 {
		return fmt.Errorf("empty name: %w", ErrValidationError)
	}
	if strings.Contains(name, kv.PathDelimiter) {
		return fmt.Errorf("name contains delimiter %s: %w", kv.PathDelimiter, ErrValidationError)
	}
	return nil
}

func ValidateActionName(name string) error {
	return permissions.IsValidAction(name)
}

func ValidateArn(name string) error {
	if !arn.IsARN(name) && name != permissions.All {
		return fmt.Errorf("%w: ARN '%s'", ErrValidationError, name)
	}
	if name == permissions.All {
		return nil
	}
	parsed, err := arn.Parse(name)
	if err != nil {
		return fmt.Errorf("%w: ARN '%s'", ErrValidationError, name)
	}
	if parsed.Partition == "sghub" && parsed.Service == "fs" {
		if err := validateFSArnResource(parsed.Resource); err != nil {
			return fmt.Errorf("%w: ARN '%s'", err, name)
		}
	}
	return nil
}

func validateFSArnResource(resource string) error {
	if resource == permissions.All || !strings.HasPrefix(resource, "repository/") {
		return nil
	}
	parts := strings.Split(resource, "/")
	if len(parts) < 3 || parts[1] == "" || parts[2] == "" {
		return ErrValidationError
	}
	if len(parts) == 3 {
		return nil
	}
	switch parts[3] {
	case "object":
		if len(parts) < 5 {
			return ErrValidationError
		}
	case "branch", "tag":
		if len(parts) != 5 || parts[4] == "" {
			return ErrValidationError
		}
	default:
		return ErrValidationError
	}
	return nil
}

func ValidateStatementEffect(effect string) error {
	if effect != StatementEffectDeny && effect != StatementEffectAllow {
		return fmt.Errorf("%w: effect '%s'", ErrValidationError, effect)
	}
	return nil
}
