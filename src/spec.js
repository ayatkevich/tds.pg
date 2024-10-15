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
    const table = new Table(sql, "public", "no primary key", "state");
    await expect(table.setup(X)).rejects.toThrow("tds_setup");
  });

  const table = new Table(sql, "public", "compatible", "state");

  test("with errors", async () => {
    await table.setup(X);

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

      await setTimeout(1);

      expect(fn).toHaveBeenNthCalledWith(1, { reference: { id: 2 }, from: "@", to: "x" });
      expect(fn).toHaveBeenNthCalledWith(2, { reference: { id: 2 }, from: "x", to: "y" });
      expect(fn).toHaveBeenCalledTimes(2);
    } finally {
      await unlisten();
    }
  });

  test("without errors", async () => {
    await table.setup(X, { noErrors: true });

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

      await setTimeout(1);

      expect(fn).toHaveBeenNthCalledWith(1, { reference: { id: 2 }, from: "@", to: "x" });
      expect(fn).toHaveBeenNthCalledWith(2, { reference: { id: 2 }, from: "x", to: "y" });
      expect(fn).toHaveBeenCalledTimes(2);
    } finally {
      await unlisten();
    }
  });
});
