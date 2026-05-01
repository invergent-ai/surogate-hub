package apigen

import "strings"

func SplitRepositoryID(repository string) (string, string) {
	owner, name, _ := strings.Cut(repository, "/")
	return owner, name
}

func RepositoryOwner(repository string) string {
	owner, _, _ := strings.Cut(repository, "/")
	return owner
}

func RepositoryName(repository string) string {
	_, name, _ := strings.Cut(repository, "/")
	return name
}
