import { Step } from "tds.ts";

export class Table {
  constructor(sql, { schema, table, column, program }) {
    this.sql = sql;
    this.schema = schema;
    this.table = table;
    this.column = column;
    /** @type {import("tds.ts").AnyProgram} */
    this.program = program;
  }

  async setup({ noErrors = false } = {}) {
    return this.sql`
      select "tds_setup"(
        "schema" => ${this.schema},
        "table" => ${this.table},
        "column" => ${this.column},
        "transitions" => ${this.program.transitions},
        "no_errors" => ${noErrors}
      )
    `;
  }

  async testOutputs() {
    for (const trace of this.program.traces) {
      for (const step of trace.steps) {
        if (!(step instanceof Step)) continue;
        const { output } = step.options;
        if (typeof output === "undefined") throw new Error("step has no output");

        // try populating a record with the output
        await this.sql`
          select jsonb_populate_record(
            null::${this.sql(this.schema)}.${this.sql(this.table)},
            ${output}::jsonb
          )
        `;
      }
    }
  }
}
