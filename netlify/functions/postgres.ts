import { Handler } from "@netlify/functions";
import { Request, Server } from "./protocol";
import { Connection } from "postgresql-client";

const server = (postgres) => {
  return {
    test_connection: async (request) => {
      const result = await postgres.query("select 1 as one");
      const rows = result.rows;
      return { success: true };
    },

    list_objects: async (request) => {
      const result = await postgres.query("select table_schema, table_name from information_schema.tables where table_schema NOT IN ('pg_catalog', 'information_schema')");
      console.log(result.rows);
      const objects = result.rows.map(cols => {
        const object_api_name = `${cols[0]}.${cols[1]}`;
        return { object_api_name, label: object_api_name };
      });
      return { objects };
    },

    supported_operations: async (request) => {
      return { operations: ["upsert"] };
    },

    list_fields: async (request) => {
      const [schema, table] = request.object.object_api_name.split(".");

      // Look up constraints (unique or primary key) for this table
      // TODO: could probably combine this with the query below and have the DB do this join for us
      const constraints_query = await postgres.prepare('select column_name, constraint_type from information_schema.constraint_column_usage natural join information_schema.table_constraints where table_schema = $1 and table_name = $2');
      const constraints_by_column_name = {};
      (await constraints_query.execute({objectRows: true, params: [schema, table]})).rows.forEach(row => {
        const column_name = row.column_name;
        if (!constraints_by_column_name[column_name]) {
          constraints_by_column_name[column_name] = [];
        }
        constraints_by_column_name[column_name].push(row.constraint_type);
      });

      await constraints_query.close();

      // Now look up the actual columns
      const statement = await postgres.prepare('select column_name, data_type, is_nullable, is_updatable from information_schema.columns where table_schema = $1 and table_name = $2')
      const result = await statement.execute({objectRows: true, params: [schema, table]});

      const fields = result.rows.map(row => {
        let type;
        if (row.data_type == 'character varying') {
          type = 'string';
        } else if (row.data_type == 'integer') {
          type = 'integer';
        } else if (row.data_type == 'boolean') {
          type = 'boolean';
        } else {
          // TODO handle other postgres types
          type = 'string';
        }

        const constraints = constraints_by_column_name[row.column_name] || [];
        const identifier = constraints.includes('UNIQUE') || constraints.includes('PRIMARY KEY');
        const required = row.is_nullable !== 'YES';
        const updateable = row.is_updatable === 'YES';

        return {
          field_api_name: row.column_name,
          label: row.column_name,
          createable: true, // TODO not for autonumber / serial columns
          array: false, // TODO check data types
          identifier,
          required,
          updateable,
          type,
        };
      });

      await statement.close();

      return { fields };
    },

    get_sync_speed: async (request) => {
      return {
        maximum_batch_size: 1000,
        maximum_records_per_second: 100000,
        maximum_parallel_batches: 8,
      }
    },

    sync_batch: async (request) => {
      const sync_plan = request.sync_plan;
      const qualified_table_name = sync_plan.object.object_api_name;

      // TODO implement other operations - insert and update

      // NOTE: your table must have a unique constraint on the identifier column
      // in order for this upsert statement builder to work
      const schema = Object.values(sync_plan.schema);
      const key_column = schema.find(v => v.active_identifier).field.field_api_name;
      const other_columns = schema.filter(v => !v.active_identifier).map(v => v.field.field_api_name);
      const all_columns = [key_column].concat(other_columns);

      // WARNING: there's a risk of SQL injection in the table and column name
      // quoting (or lack thereof) here
      let query = `insert into ${qualified_table_name} (${all_columns.join(',')}) values (`;
      query += all_columns.map((_, index) => `$${index + 1}`).join(',');
      query += `) on conflict (${key_column}) do update set `
      query += other_columns.map(column => `${column}=excluded.${column}`).join(",");

      // Execute the prepared query once per record in a transaction
      await postgres.startTransaction();
      let statement = await postgres.prepare(query);
      // TODO wrap in a transaction
      const insert_promises = request.records.map(r => {
        const params = all_columns.map(column => r[column]);
        console.log({ params });
        statement.execute({ params });
      });
      await Promise.all(insert_promises);
      await postgres.commit();
      await statement.close();

      const record_results = request.records.map(r => {
        return {
          success: true,
          identifier: r[key_column],
        }
      });

      return { record_results };
    }
  } as Server;
}

const handler: Handler = async (event, context) => {
  const request = JSON.parse(event.body);
  const method = request.method;

  const connection = new Connection();
  await connection.connect();

  try {
    const result = await server(connection)[method](request.params);

    const response = {
      jsonrpc: "2.0",
      id: request.id,
      result,
    }

    return {
      statusCode: 200,
      body: JSON.stringify(response),
    };
  } finally {
    await connection.close();
  }
};

export { handler };
