import { afterAll, beforeAll, beforeEach, describe, expect, jest, test } from "@jest/globals";
import postgres from "postgres";
import { Implementation, Program, Trace } from "tds.ts";
import { setTimeout } from "timers/promises";
import { Table } from "./index.js";

describe("tds.pg", () => {
  const sql = postgres();
  beforeAll(() => sql.file("src/tds-pg.sql"));
  afterAll(() => sql.end());

  const X = new Program([
    new Trace("trace") //
      .step("@", { output: { state: "x" } })
      .step("x", { output: { state: "y" } })
      .step("y", { output: { state: "y" } }),
  ]);

  const x = new Implementation(X)
    .transition("@", "x", ({ state }) => {
      return ["y", { state: "y" }];
    })
    .transition("x", "y", (row) => {
      return ["@", row];
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

  const table = new Table(sql, {
    schema: "public",
    table: "compatible",
    column: "state",
    program: X,
  });

  test("implementation", async () => {
    await x.test();
    await table.setup();
    await table.testOutputs();
  });

  test("no primary key", async () => {
    await expect(
      new Table(sql, {
        schema: "public",
        table: "no primary key",
        column: "state",
        program: X,
      }).setup(),
    ).rejects.toThrow("tds_setup");
  });

  test("with errors", async () => {
    await table.setup();

    const fn = jest.fn();
    const { unlisten } = await sql.listen("public_compatible_state_transition", (data) =>
      fn(JSON.parse(data)),
    );
    try {
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

      await setTimeout(10);

      expect(fn).toHaveBeenNthCalledWith(1, { reference: { id: 2 }, from: "@", to: "x" });
      expect(fn).toHaveBeenNthCalledWith(2, { reference: { id: 2 }, from: "x", to: "y" });
      expect(fn).toHaveBeenCalledTimes(2);
    } finally {
      await unlisten();
    }
  });

  test("without errors", async () => {
    await table.setup({ noErrors: true });

    const fn = jest.fn();
    const { unlisten } = await sql.listen("public_compatible_state_transition", (data) =>
      fn(JSON.parse(data)),
    );
    try {
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

      await setTimeout(10);

      expect(fn).toHaveBeenNthCalledWith(1, { reference: { id: 2 }, from: "@", to: "x" });
      expect(fn).toHaveBeenNthCalledWith(2, { reference: { id: 2 }, from: "x", to: "y" });
      expect(fn).toHaveBeenCalledTimes(2);
    } finally {
      await unlisten();
    }
  });

  test("handling", async () => {
    await table.setup();

    const stop = await table.handle(x);

    try {
      await sql`
        insert into "compatible" ("state") values ('x')
      `;

      await setTimeout(10);

      await expect(sql`
        select * from "compatible"
      `).resolves.toEqual([{ id: 1, state: "y" }]);
    } finally {
      await stop();
    }
  });
});
