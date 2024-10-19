import type { Sql } from "postgres";
import { AnyProgram, Implementation } from "tds.ts";

export declare class Table<T extends AnyProgram> {
  constructor(sql: Sql, options: { schema: string; table: string; column: string; program: T });

  channel: string;
  setup(options?: { noErrors: boolean }): Promise<void>;
  testOutputs(): Promise<void>;
  listen(
    fn: (data: {
      reference: Record<string, unknown>;
      data: { from: string; to: string; record: Record<string, unknown> };
    }) => Promise<void>,
  ): Promise<() => Promise<void>>;
  handle(implementation: Implementation<T>): Promise<() => Promise<void>>;
}
