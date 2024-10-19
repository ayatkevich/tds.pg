create or replace function "tds_check_transition"() returns trigger as $$
  declare
    "~column" text = tg_argv[0];
    "~transitions" text[][] = tg_argv[1];
    "~noErrors" boolean = tg_argv[2];
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
      if "~noErrors" then
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
    "~notificationName" text = tg_argv[0];
    "~column" text = tg_argv[1];
    "~primaryKey" text[] = tg_argv[2];
    "~old" jsonb = to_jsonb(old);
    "~new" jsonb = to_jsonb(new);
    "~notification" jsonb = jsonb_build_object(
      'from', coalesce("~old"->>"~column", '@'),
      'to', "~new"->>"~column"
    );
    "~reference" jsonb = jsonb_build_object();
    "~key" text;
  begin
    foreach "~key" in array "~primaryKey" loop
      "~reference" = "~reference" || jsonb_build_object("~key", "~new"->"~key");
    end loop;
    perform pg_notify("~notificationName", ("~notification" || jsonb_build_object('reference', "~reference"))::text);
    return new;
  end
$$ language plpgsql;

create or replace function "tds_setup"(
  "schema" text,
  "table" text,
  "column" text = 'state',
  "transitions" text[][] = array[]::text[][],
  "no_errors" boolean = false
) returns void as $$
  declare
    "~primaryKey" text[] = (
      select array_agg(attName)
        from pg_class
          inner join pg_constraint
            on conRelId = pg_class.oid
          inner join pg_attribute
            on attNum = any(conKey)
              and attRelId = pg_class.oid
        where relKind = 'r'
          and relName = "table"
          and relNamespace::regNamespace::text = "schema"
          and conType = 'p'
    );
  begin
    if "~primaryKey" is null then
      raise exception 'tds_setup: table % has no primary key', "table";
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
      "no_errors"
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
