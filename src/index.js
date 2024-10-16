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

  async handle(/** @type {import("tds.ts").Implementation} */ implementation) {
    const { unlisten } = await this.sql.listen(
      `${this.schema}_${this.table}_${this.column}_transition`,
      async (data) => {
        data = JSON.parse(data);

        const reference = Object.entries(data.reference)
          .map(([column, value]) => this.sql`${this.sql(column)} is not distinct from ${value}`)
          .reduce((where, reference) => this.sql`${where} and ${reference}`, this.sql`true`);

        await this.sql.begin(async (sql) => {
          const [input] = await sql`
            select *
              from ${sql(this.schema)}.${sql(this.table)}
              where ${reference}
                and ${this.sql(this.column)} = ${data.to}
              for update skip locked
          `;
          if (!input) return;

          const [state, output] = await implementation.execute(data.from, data.to, input);

          if (state === "@") return;

          await sql`
            update ${sql(this.schema)}.${sql(this.table)}
              set ${sql(output)}
              where ${reference}
              returning *
          `;
        });
      },
    );
    return async () => {
      await unlisten();
    };
  }
}
