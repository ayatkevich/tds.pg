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
      drop table if exists "test" cascade;
      create table "test" (
        "state" text
      );
    `.simple(),
  );

  test("implementation", async () => {
    await x.test();
  });

  test("with errors", async () => {
    await sql`
      select "tds_setup"(
        "schema" => 'public',
        "table" => 'test',
        "column" => 'state',
        "states" => ${X.states},
        "transitions" => ${X.transitions}
      )
    `;

    await expect(sql`
      insert into "test" ("state") values ('wrong')
    `).rejects.toThrow("tds_transition_check");

    await sql`
      insert into "test" ("state") values ('x')
    `;

    await expect(sql`
      update "test" set "state" = 'wrong'
    `).rejects.toThrow("tds_transition_check");

    await sql`
      update "test" set "state" = 'y'
    `;
  });

  test("without errors", async () => {
    await sql`
      select "tds_setup"(
        "schema" => 'public',
        "table" => 'test',
        "column" => 'state',
        "states" => ${X.states},
        "transitions" => ${X.transitions},
        "no_errors" => true
      )
    `;

    await expect(sql`
      insert into "test" ("state") values ('wrong') returning *
    `).resolves.toEqual([]);

    await expect(sql`
      insert into "test" ("state") values ('x') returning *
    `).resolves.toEqual([{ state: "x" }]);

    await expect(sql`
      update "test" set "state" = 'wrong' returning *
    `).resolves.toEqual([]);

    await expect(sql`
      update "test" set "state" = 'y' returning *
    `).resolves.toEqual([{ state: "y" }]);
  });
});
