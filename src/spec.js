import { afterAll, beforeAll, beforeEach, describe, expect, jest, test } from "@jest/globals";
import postgres from "postgres";
import { Implementation, Program, Trace } from "tds.ts";
import { setTimeout } from "timers/promises";
import { definitionPath, Table } from "./index.js";

describe("tds.pg", () => {
  const sql = postgres();
  beforeAll(() => sql.file(definitionPath));
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
      drop schema if exists "TestSchema" cascade;
      create schema "TestSchema";

      create table "TestSchema"."T" (
        "id" serial primary key,
        "state" text
      );

      drop table if exists "compatible" cascade;
      create table "compatible" (
        "id" serial primary key,
        "state" text
      );

      drop table if exists "composite primary ke" cascade;
      create table "composite primary ke" (
        "id" serial,
        "name" text,
        "state" text,
        primary key ("id", "name")
      );

      drop table if exists "no primary key" cascade;
      create table "no primary key" (
        "state" text
      );

      drop table if exists "no column" cascade;
      create table "no column" (
        "id" serial primary key
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
    ).rejects.toThrow(`tds_setup: table "public"."no primary key" has no primary key`);
  });

  test("no table", async () => {
    await expect(
      new Table(sql, {
        schema: "public",
        table: "no table",
        column: "state",
        program: X,
      }).setup(),
    ).rejects.toThrow(`tds_setup: table "public"."no table" does not exist`);
  });

  test("no column", async () => {
    await expect(
      new Table(sql, {
        schema: "public",
        table: "no column",
        column: "state",
        program: X,
      }).setup(),
    ).rejects.toThrow(`tds_setup: table "public"."no column" has no state column "state"`);
  });

  test("different schema", async () => {
    const table = new Table(sql, {
      schema: "TestSchema",
      table: "T",
      column: "state",
      program: X,
    });

    await table.setup();
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

      expect(fn.mock.calls).toEqual([
        [
          {
            channel: "public_compatible_state_transition",
            from: "@",
            to: "x",
            reference: { id: 2 },
            old: null,
            new: { id: 2, state: "x" },
            state: "received",
            id: expect.any(String),
          },
        ],
        [
          {
            channel: "public_compatible_state_transition",
            from: "x",
            to: "y",
            reference: { id: 2 },
            old: { id: 2, state: "x" },
            new: { id: 2, state: "y" },
            state: "received",
            id: expect.any(String),
          },
        ],
      ]);
    } finally {
      await stop();
    }
  });

  test("without errors", async () => {
    await table.setup({ silent: true });

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

      expect(fn.mock.calls).toEqual([
        [
          {
            channel: "public_compatible_state_transition",
            from: "@",
            to: "x",
            reference: { id: 2 },
            old: null,
            new: { id: 2, state: "x" },
            state: "received",
            id: expect.any(String),
          },
        ],
        [
          {
            channel: "public_compatible_state_transition",
            from: "x",
            to: "y",
            reference: { id: 2 },
            old: { id: 2, state: "x" },
            new: { id: 2, state: "y" },
            state: "received",
            id: expect.any(String),
          },
        ],
      ]);
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

  test("composite primary ke", async () => {
    const table = new Table(sql, {
      schema: "public",
      table: "composite primary ke",
      column: "state",
      program: X,
    });

    await table.setup();

    const stop = await table.handle(x);

    try {
      await sql`
        insert into "composite primary ke" ("id", "name", "state") values (1, 'a', 'x')
      `;

      await setTimeout(10);

      await expect(sql`
        select * from "composite primary ke"
      `).resolves.toEqual([{ id: 1, name: "a", state: "y" }]);
    } finally {
      await stop();
    }
  });
});
