import { afterAll, beforeAll, beforeEach, describe, expect, test } from "@jest/globals";
import postgres from "postgres";
import { Implementation, Program, Trace } from "tds.ts";

describe("tds.pg", () => {
  const sql = postgres();
  beforeAll(() => sql.file("src/tds-pg.sql"));
  afterAll(() => sql.end());

  const X = new Program([
    new Trace("trace") //
      .step("@", { output: { state: "x" } })
      .step("x", { output: { state: "y" } })
      .step("y"),
  ]);

  const x = new Implementation(X)
    .transition("@", "x", ({ state }) => {
      return ["y", { state: "y" }];
    })
    .transition("x", "y", ({ state }) => {
      return ["@", { state }];
    });

  beforeEach(() =>
    sql`
      drop table if exists "compatible" cascade;
      create table "compatible" (
        "id" serial primary key,
        "state" text
      );

      drop table if exists "no primary key" cascade;
      create table "no primary key" (
        "state" text
      );
    `.simple(),
  );

  test("implementation", async () => {
    await x.test();
  });

  test("no primary key", async () => {
    await expect(sql`
      select "tds_setup"(
        "schema" => 'public',
        "table" => 'no primary key',
        "column" => 'state',
        "states" => ${X.states},
        "transitions" => ${X.transitions}
      )
    `).rejects.toThrow("tds_setup");
  });

  test("with errors", async () => {
    await sql`
      select "tds_setup"(
        "schema" => 'public',
        "table" => 'compatible',
        "column" => 'state',
        "states" => ${X.states},
        "transitions" => ${X.transitions}
      )
    `;

    await expect(sql`
      insert into "compatible" ("state") values ('wrong')
    `).rejects.toThrow("tds_transition_check");

    await sql`
      insert into "compatible" ("state") values ('x')
    `;

    await expect(sql`
      update "compatible" set "state" = 'wrong'
    `).rejects.toThrow("tds_transition_check");

    await sql`
      update "compatible" set "state" = 'y'
    `;
  });

  test("without errors", async () => {
    await sql`
      select "tds_setup"(
        "schema" => 'public',
        "table" => 'compatible',
        "column" => 'state',
        "states" => ${X.states},
        "transitions" => ${X.transitions},
        "no_errors" => true
      )
    `;

    await expect(sql`
      insert into "compatible" ("state") values ('wrong') returning *
    `).resolves.toEqual([]);

    await expect(sql`
      insert into "compatible" ("state") values ('x') returning *
    `).resolves.toEqual([{ id: 2, state: "x" }]);

    await expect(sql`
      update "compatible" set "state" = 'wrong' returning *
    `).resolves.toEqual([]);

    await expect(sql`
      update "compatible" set "state" = 'y' returning *
    `).resolves.toEqual([{ id: 2, state: "y" }]);
  });
});
