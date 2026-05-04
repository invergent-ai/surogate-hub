def split_repository_id(repository: str) -> tuple[str, str]:
    """Split an ``owner/repository`` id for generated API methods."""
    owner, sep, name = repository.partition("/")
    if not sep or not owner or not name:
        raise ValueError("repository must be in '<owner>/<repository>' form")
    return owner, name
