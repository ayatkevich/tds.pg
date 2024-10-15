export class Table {
  constructor(sql, schema, table, column) {
    this.sql = sql;
    this.schema = schema;
    this.table = table;
    this.column = column;
  }

  async setup(program, { noErrors = false } = {}) {
    return this.sql`
      select "tds_setup"(
        "schema" => ${this.schema},
        "table" => ${this.table},
        "column" => ${this.column},
        "transitions" => ${program.transitions},
        "no_errors" => ${noErrors}
      )
    `;
  }
}
