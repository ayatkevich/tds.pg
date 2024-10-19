# tds.pg

A PostgreSQL implementation for Trace-Driven State Machines (TDS).

## Description

tds.pg is a Node.js module that provides a PostgreSQL implementation for Trace-Driven State Machines. It allows you to set up, listen to, and handle state transitions in a PostgreSQL database using the TDS approach.

## Installation

To install the package, run:

```bash
npm install tds.pg
```

## Dependencies

- tds.ts: ^0.15.0
- postgres: ^3.4.4

## Usage

Here's a basic example of how to use the `Table` class:

```javascript
import { Table } from "tds.pg";
import postgres from "postgres";
import { Program, Trace, Implementation } from "tds.ts";

// Create a PostgreSQL connection
const sql = postgres();

// Define your program
const X = new Program([
  new Trace("trace")
    .step("@", { output: { record: { state: "x" }, sql } })
    .step("x", { output: { record: { state: "y" }, sql } })
    .step("y", { output: { record: { state: "y" }, sql } }),
]);

// Create an implementation
const x = new Implementation(X)
  .transition("@", "x", async (it) => {
    return ["y", { ...it, record: { state: "y" } }];
  })
  .transition("x", "y", async (it) => {
    return ["@", it];
  });

// Create a Table instance
const table = new Table(sql, {
  schema: "public",
  table: "your_table_name",
  column: "state",
  program: X,
});

// Setup the table
await table.setup();

// Handle transitions
const stop = await table.handle(x);

// ... Later, when you want to stop handling
await stop();
```

## API

### `Table` class

The main class for interacting with the PostgreSQL implementation of TDS.

#### Constructor

```javascript
new Table(sql, options);
```

- `sql`: A postgres-js SQL instance
- `options`: An object containing:
  - `schema`: The database schema name
  - `table`: The table name
  - `column`: The column name for the state
  - `program`: A TDS Program instance

#### Methods

- `setup(options)`: Sets up the necessary triggers and functions in the database
- `testOutputs()`: Tests the outputs of the program against the database schema
- `listen(fn)`: Listens for state transitions and calls the provided function
- `handle(implementation)`: Handles state transitions using the provided implementation

## Testing

The project uses Jest for testing. To run the tests:

```bash
npm test
```

## License

This project is licensed under the MIT License.

## Contributing

Contributions are welcome. Please submit pull requests or open issues on the project's GitHub repository.
