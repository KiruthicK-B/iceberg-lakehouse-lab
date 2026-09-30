{#
  dbt-trino's default `table` materialization does rename-to-backup + create-
  new + drop-backup on every re-run (only a first-ever creation skips this).
  Polaris disables purge-on-drop by default (DROP TABLE on an existing table
  needs purgeRequested, which Polaris 403s without explicit server config),
  so every second-and-later run of any table-materialized model fails.

  Trino/Iceberg supports CREATE OR REPLACE TABLE natively (atomic swap, no
  drop involved) -- use that directly instead of the backup/drop dance.
#}
{% materialization table, adapter='trino' %}

  {%- set target_relation = this.incorporate(type='table') -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% call statement('main') -%}
    CREATE OR REPLACE TABLE {{ target_relation }} AS
    {{ sql }}
  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
