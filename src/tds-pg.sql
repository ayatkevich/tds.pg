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

create or replace function "tds_setup"(
  "table" text,
  "column" text = 'state',
  "states" text[] = array[]::text[],
  "transitions" text[][] = array[]::text[][],
  "no_errors" boolean = false
) returns void as $$
  begin
    execute format(
      $query$
        create trigger "tds_transition_check" before insert or update on %I
        for each row
        execute procedure "tds_check_transition"(%L, %L, %L)
      $query$,
      "table",
      "column",
      "transitions",
      "no_errors"
    );
  end
$$ language plpgsql;
