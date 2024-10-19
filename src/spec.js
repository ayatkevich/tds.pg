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
      .step("@", () => ({ output: { record: { state: "x" }, sql } }))
      .step("x", () => ({ output: { record: { state: "y" }, sql } }))
      .step("y", () => ({ output: { record: { state: "y" }, sql } })),
  ]);

  const x = new Implementation(X)
    .transition("@", "x", async (it) => {
      return ["y", { ...it, record: { state: "y" } }];
    })
    .transition("x", "y", async (it) => {
      return ["@", it];
    });

  beforeEach(() =>
    sql`
      drop table if exists "compatible" cascade;
      create table "compatible" (
        "id" serial primary key,
        "state" text
      );

      drop table if exists "composite primary key" cascade;
      create table "composite primary key" (
        "id" serial,
        "name" text,
        "state" text,
        primary key ("id", "name")
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
    const stop = await table.listen(fn);

    try {
      await expect(sql`
        insert into "compatible" ("state") values ('wrong')
      `).rejects.toThrow("tds_transition_check");

      await sql`
        insert into "compatible" ("state") values ('x')
      `;

      await setTimeout(10);

      await expect(sql`
        update "compatible" set "state" = 'wrong'
      `).rejects.toThrow("tds_transition_check");

      await sql`
        update "compatible" set "state" = 'y'
      `;

      await setTimeout(10);

      expect(fn).toHaveBeenNthCalledWith(1, {
        reference: { id: 2 },
        record: { id: 2, state: "x" },
        from: "@",
        to: "x",
      });
      expect(fn).toHaveBeenNthCalledWith(2, {
        reference: { id: 2 },
        record: { id: 2, state: "y" },
        from: "x",
        to: "y",
      });
      expect(fn).toHaveBeenCalledTimes(2);
    } finally {
      await stop();
    }
  });

  test("without errors", async () => {
    await table.setup({ noErrors: true });

    const fn = jest.fn();
    const stop = await table.listen(fn);

    try {
      await expect(sql`
      insert into "compatible" ("state") values ('wrong') returning *
    `).resolves.toEqual([]);

      await expect(sql`
      insert into "compatible" ("state") values ('x') returning *
    `).resolves.toEqual([{ id: 2, state: "x" }]);

      await setTimeout(10);

      await expect(sql`
      update "compatible" set "state" = 'wrong' returning *
    `).resolves.toEqual([]);

      await expect(sql`
      update "compatible" set "state" = 'y' returning *
    `).resolves.toEqual([{ id: 2, state: "y" }]);

      await setTimeout(10);

      expect(fn).toHaveBeenNthCalledWith(1, {
        reference: { id: 2 },
        record: { id: 2, state: "x" },
        from: "@",
        to: "x",
      });
      expect(fn).toHaveBeenNthCalledWith(2, {
        reference: { id: 2 },
        record: { id: 2, state: "y" },
        from: "x",
        to: "y",
      });
      expect(fn).toHaveBeenCalledTimes(2);
    } finally {
      await stop();
    }
  });

  test("handling", async () => {
    await table.setup();

    const stop1 = await table.handle(x);
    const stop2 = await table.handle(x);

    try {
      await sql`
        insert into "compatible" ("state") values ('x')
      `;

      await setTimeout(10);

      await expect(sql`
        select * from "compatible"
      `).resolves.toEqual([{ id: 1, state: "y" }]);
    } finally {
      await stop1();
      await stop2();
    }
  });

  test("composite primary key", async () => {
    const table = new Table(sql, {
      schema: "public",
      table: "composite primary key",
      column: "state",
      program: X,
    });

    await table.setup();

    const fn = jest.fn();
    const { unlisten } = await sql.listen(table.channel, (data) => fn(JSON.parse(data)));

    const stop = await table.handle(x);

    try {
      await sql`
        insert into "composite primary key" ("id", "name", "state") values (1, 'a', 'x')
      `;

      await setTimeout(10);

      await expect(sql`
        select * from "composite primary key"
      `).resolves.toEqual([{ id: 1, name: "a", state: "y" }]);

      expect(fn).toHaveBeenCalledTimes(2);
      expect(fn).toHaveBeenNthCalledWith(1, {
        reference: { id: 1, name: "a" },
        from: "@",
        to: "x",
      });
      expect(fn).toHaveBeenNthCalledWith(2, {
        reference: { id: 1, name: "a" },
        from: "x",
        to: "y",
      });
    } finally {
      await stop();
      await unlisten();
    }
  });
});
