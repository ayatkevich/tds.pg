import { afterAll, beforeAll, describe, expect, test } from "@jest/globals";
import postgres from "postgres";
import { Implementation, Program, Trace } from "tds.ts";

describe("tds.pg", () => {
  const sql = postgres();
  beforeAll(() => sql.file("src/tds-pg.sql"));
  afterAll(() => sql.end());

  test("", async () => {
    const X = new Program([new Trace("trace").step("@").step("x").step("y")]);
    const x = new Implementation(X)
      .transition("@", "x", async () => {
        return ["y"];
      })
      .transition("x", "y", async () => {
        return ["@"];
      });

    await sql`
      drop table if exists "test" cascade;
      create table "test" (
        "state" text
      );
    `.simple();

    await sql`
      select "tds_setup"(
        "table" => 'test',
        "column" => 'state',
        "states" => ${X.states},
        "transitions" => ${X.transitions}
      )
    `;

    await expect(sql`
      insert into "test" ("state") values ('wrong')
    `).rejects.toThrow("tds_state_check");

    await sql`
      insert into "test" ("state") values ('x')
    `;
  });
});
