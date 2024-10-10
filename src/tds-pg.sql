create or replace function "tds_setup"(
  "table" text,
  "column" text = 'state',
  "states" text[] = array[]::text[],
  "transitions" text[][] = array[]::text[][]
) returns void as $$
  begin
    -- setup state check
    execute format(
      $query$
        alter table %I add constraint "tds_state_check" check ("%I" in (%s))
      $query$,
      "table",
      "column",
      (select string_agg(format('%L', "state"), ', ')
        from unnest("states") as "state")
    );
  end
$$ language plpgsql;
