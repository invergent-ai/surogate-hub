# Project-Scoped Repository Access

This guideline describes how an external project system can use Surogate Hub
namespaced repositories and ACL policies to give users access to one or more
project repositories.

## Core Model

Use Surogate Hub as the repository and data plane. Keep project ownership,
friendly names, billing, invitations, and membership state in the external
application.

Repository IDs are exactly `owner/repo`. For project-based access, make the
first segment a stable project namespace rather than a human username:

```text
p-7f3a/datasets
p-7f3a/models
p-7f3a/checkpoints
p-9ab2/datasets
```

The external system owns the mapping:

```text
Project "Cancer Study" -> hub namespace p-7f3a
Project repo "datasets" -> hub repo p-7f3a/datasets
```

Use opaque, stable project IDs for the Hub namespace. Store user-facing project
names in the external system.

## Authorization

Do not use the built-in ACL groups for project-scoped access. ACL permissions
such as `Read`, `Write`, `Super`, and `Admin` expand to global resources and
are useful for simple instance-wide roles, not per-project membership.

Use custom Hub policies and groups per project or per project repository.

Project-wide access:

```text
group: project_p7f3a_read
resource: arn:sghub:fs:::repository/p-7f3a/*

group: project_p7f3a_write
resource: arn:sghub:fs:::repository/p-7f3a/*
```

Single-repository access:

```text
group: project_p7f3a_datasets_read
resource: arn:sghub:fs:::repository/p-7f3a/datasets
resource: arn:sghub:fs:::repository/p-7f3a/datasets/object/*
resource: arn:sghub:fs:::repository/p-7f3a/datasets/branch/*
resource: arn:sghub:fs:::repository/p-7f3a/datasets/tag/*
```

Prefer exact repository ARNs where possible. If using namespace wildcards, use
the slash-delimited form:

```text
arn:sghub:fs:::repository/p-7f3a/*
```

Avoid broad prefix patterns such as:

```text
arn:sghub:fs:::repository/p-7f3a*
```

Hub wildcard matching is string-based, and `*` crosses `/`. A broad prefix
pattern can match unintended project namespaces.

## Invitation Flow

1. A user creates a project in the external application.
2. The application allocates a stable Hub namespace, for example `p-7f3a`.
3. The application creates Hub repositories under that namespace.
4. The application creates Hub groups and scoped policies for the project.
5. When inviting a user, the application provisions or finds the Hub user.
6. The application adds that Hub user to the correct project group.
7. Revocation removes the user from the project group.

Project owners should be owners in the external application. Do not grant Hub
`Admin` for project ownership; Hub `Admin` is global.

## Recommended Roles

Use these project roles in the external application:

```text
viewer       -> read/list Hub actions
contributor  -> read/list/write object plus branch and commit actions
owner        -> contributor in Hub plus project administration in the external app
```

The external application should hold a Hub service account with permission to
manage users, groups, and policies. Normal users should receive only scoped
repository permissions through group membership.

This design supports multiple projects per user, invitations to one project or
to selected repositories within a project, and isolation between project
namespaces.
