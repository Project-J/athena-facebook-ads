# athena-facebook-ads
This [dbt](https://github.com/fishtown-analytics/dbt) package contains macros
that:

- can be (re)used across dbt projects running on Athena
- define Athena-specific implementations of [dispatched macros](https://docs.getdbt.com/reference/dbt-jinja-functions/adapter/#dispatch) from other packages

## Installation Instructions

Add to your packages.yml

```yaml
packages:
  - git: https://github.com/Project-J/athena-facebook-ads
    revision: 0.1.0
```

For dbt < v0.19.2, add the following lines to your `dbt_project.yml`:

```yaml
vars:
  dbt_utils_dispatch_list: ["athena_facebook_ads"]
```

For dbt >= v0.19.2, , add the following lines to your `dbt_project.yml`:

```yaml
dispatch:
  - macro_namespace: facebook_ads
    search_order: [athena_facebook_ads, facebook_ads]
```

## Compatibility

This package provides "shims" for [`facebook_ads`](https://github.com/dbt-labs/facebook-ads).
In the future more shims could be added to this repository.

### Contributing

We welcome contributions to this repo! To contribute a new feature or a fix,
please open a Pull Request with 1) your changes and 2) updated documentation for
the `README.md` file.
