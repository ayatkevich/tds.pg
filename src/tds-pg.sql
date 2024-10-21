drop table if exists "tds_messages";
create table "tds_messages" (
  "channel" text,
  "from" text,
  "to" text,
  "reference" jsonb,
  "old" jsonb,
  "new" jsonb,
  "state" text default 'sent',
  "id" uuid primary key default gen_random_uuid()
);

create or replace function "tds_check_transition"() returns trigger as $$
  declare
    "~column" text = tg_argv[0];
    "~transitions" text[][] = tg_argv[1];
    "~silent" boolean = tg_argv[2];
    "~old" jsonb = to_jsonb(old);
    "~new" jsonb = to_jsonb(new);
  begin
    if array["~old"->>"~column", "~new"->>"~column"] not in (
      select
          array[
            case when "~transition"->>0 = '@' then null else "~transition"->>0 end,
            "~transition"->>1
          ]
        from jsonb_array_elements(to_jsonb("~transitions")) as "~transition"
    ) then
      if "~silent" then
        return null;
      else
        raise exception 'tds_transition_check: incorrect transition from % to %',
          "~old"->>"~column",
          "~new"->>"~column";
      end if;
    end if;
    return new;
  end
$$ language plpgsql;

create or replace function "tds_notify_transition"() returns trigger as $$
  declare
    "~channel" text = tg_argv[0];
    "~column" text = tg_argv[1];
    "~primaryKey" text[] = tg_argv[2];
    "~old" jsonb = to_jsonb(old);
    "~new" jsonb = to_jsonb(new);
    "~reference" jsonb = '{}'::jsonb;
    "~key" text;
    "~messageId" uuid;
  begin
    foreach "~key" in array "~primaryKey" loop
      "~reference" = "~reference" || jsonb_build_object("~key", "~new"->"~key");
    end loop;

    insert into "tds_messages"
      values (
        "~channel",
        coalesce("~old"->>"~column", '@'),
        "~new"->>"~column",
        "~reference",
        "~old",
        "~new"
      )
      returning "id"
      into "~messageId";

    raise notice '~messageId: %', "~messageId";

    perform pg_notify("~channel", "~messageId"::text);
    return new;
  end
$$ language plpgsql;

create or replace function "tds_setup"(
  "schema" text,
  "table" text,
  "column" text = 'state',
  "transitions" text[][] = array[]::text[][],
  "silent" boolean = false
) returns void as $$
  declare
    "~primaryKey" text[] = (
      select array_agg(attName)
        from pg_class
          inner join pg_namespace
            on relNamespace = pg_namespace.oid
          inner join pg_constraint
            on conRelId = pg_class.oid
          inner join pg_attribute
            on attNum = any(conKey)
              and attRelId = pg_class.oid
        where relKind = 'r'
          and nspName = "schema"
          and relName = "table"
          and conType = 'p'
    );
  begin
    if not exists (
      select
        from pg_class
          inner join pg_namespace
            on relNamespace = pg_namespace.oid
          where nspName = "schema"
            and relName = "table"
    ) then
      raise exception 'tds_setup: table "%"."%" does not exist', "schema", "table";
    end if;

    if not exists (
      select
        from pg_attribute
          inner join pg_class
            on attRelId = pg_class.oid
          inner join pg_namespace
            on relNamespace = pg_namespace.oid
          where nspName = "schema"
            and relName = "table"
            and attName = "column"
    ) then
      raise exception 'tds_setup: table "%"."%" has no state column "%"', "schema", "table", "column";
    end if;

    if "~primaryKey" is null then
      raise exception 'tds_setup: table "%"."%" has no primary key', "schema", "table";
    end if;

    execute format(
      $query$
        create trigger "tds_transition_check" before insert or update on %I.%I
        for each row
        execute procedure "tds_check_transition"(%L, %L, %L)
      $query$,
      "schema",
      "table",
      "column",
      "transitions",
      "silent"
    );

    execute format(
      $query$
        create trigger "tds_transition_notify" after insert or update on %I.%I
        for each row
        execute procedure "tds_notify_transition"(%L, %L, %L)
      $query$,
      "schema",
      "table",
      format('%s_%s_%s_transition', "schema", "table", "column"),
      "column",
      "~primaryKey"
    );
  end
$$ language plpgsql;
