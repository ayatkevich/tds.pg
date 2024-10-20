import { readFile } from "fs/promises";
import path from "path";
import { Step } from "tds.ts";
import { fileURLToPath } from "url";

const dirname = fileURLToPath(new URL(".", import.meta.url));

export const definitionPath = path.join(dirname, "tds-pg.sql");

export const definition = await readFile(definitionPath, "utf8");

export class Table {
  constructor(sql, { schema, table, column, program }) {
    this.sql = sql;
    this.schema = schema;
    this.table = table;
    this.column = column;
    /** @type {import("tds.ts").AnyProgram} */
    this.program = program;
  }

  get channel() {
    return `${this.schema}_${this.table}_${this.column}_transition`;
  }

  async setup({ silent = false } = {}) {
    return this.sql`
      select "tds_setup"(
        "schema" => ${this.schema},
        "table" => ${this.table},
        "column" => ${this.column},
        "transitions" => ${this.program.transitions},
        "silent" => ${silent}
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

  async listen(fn) {
    return this.#listen(async ({ data }) => {
      await fn(data);
    });
  }

  async handle(/** @type {import("tds.ts").Implementation} */ implementation) {
    return this.#listen(async ({ sql, reference, data }) => {
      const [state, { record }] = await implementation.execute(data.from, data.to, {
        record: data.new,
        sql,
      });

      if (state === "@") return;

      await sql`
        update ${sql(this.schema)}.${sql(this.table)}
          set ${sql(record)}
          where ${reference}
          returning *
      `;
    });
  }

  async #listen(fn) {
    const { unlisten } = await this.sql.listen(this.channel, async (id) => {
      await this.sql.begin(async (sql) => {
        const [data] = await sql`
          update "tds_messages"
            set "state" = 'received'
            where "id" = ${id}
              and "state" = 'sent'
            returning *
        `;
        if (!data) return;

        const reference = Object.entries(data.reference)
          .map(([column, value]) => this.sql`${this.sql(column)} is not distinct from ${value}`)
          .reduce((where, reference) => this.sql`${where} and ${reference}`, this.sql`true`);

        await fn({ sql, reference, data });
      });
    });
    return async () => {
      await unlisten();
    };
  }
}
