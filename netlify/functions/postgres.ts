import { Handler } from "@netlify/functions";
import { Request, Server } from "./protocol";
import { Connection } from 'postgresql-client';

const server = (postgres) => {
  return {
    test_connection: async (request) => {
      const result = await postgres.query('select 1 as one');
      const rows = result.rows;
      return { success: true };
    },

    list_objects: async (request) => {
      const result = await postgres.query("select table_schema, table_name from information_schema.tables where table_schema NOT IN ('pg_catalog', 'information_schema')");
      console.log(result.rows);
      const objects = result.rows.map(cols => {
        const object_api_name = `${cols[0]}.${cols[1]}`;
        return {object_api_name, label: object_api_name};
      });
      return { objects };
    },

    supported_operations: async (request) => {
      return {operations: ['insert', 'update', 'upsert']};
    },

    list_fields: async (request) => {
      const [schema, table] = request.object.object_api_name.split(".");

      const query = `select column_name, data_type from information_schema.columns where table_schema = '${schema}' and table_name = '${table}'`;
      const result = await postgres.query(query);

      const fields = result.rows.map(cols => {
        const [column_name, data_type] = cols;

        let field_type;
        if (data_type == 'character varying') {
          field_type = 'string';
        } else if (data_type == 'integer') {
          field_type = 'integer';
        } else if (data_type == 'boolean') {
          field_type = 'boolean';
        } else {
          // TODO handle other postgres types
          field_type = 'string';
        }

        return {
          field_api_name: column_name,
          label: column_name,
          identifier: true, // TODO depends on index
          required: true, // TODO depends on index / constraints
          createable: true,
          updateable: true,
          array: false,
          type: field_type,
        };
      });

      return {fields};
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
      const key_column = Object.values(sync_plan.schema).find(v => v.active_identifier).field.field_api_name;
      const other_columns = Object.values(sync_plan.schema).filter(v => !v.active_identifier).map(v => v.field.field_api_name);
      const all_columns = [key_column].concat(other_columns);

      // WARNING: this is not sanitized against SQL injection - doesn't even do basic quote escaping
      let query = `insert into ${qualified_table_name} (${all_columns.join(', ')}) values `;
      const values = request.records.map(record => "(" + all_columns.map(column => "'" + record[column] + "'").join(',') + ")").join(",");
      query += values
      query += ` on conflict (${key_column}) do update set `
      query += other_columns.map(column => `${column} = excluded.${column}`).join(",");

      // TODO: handle failure correctly (though Census will implicitly do the
      // right thing if we throw an error here)
      await postgres.query(query);

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

  const connection = new Connection({
    host: 'localhost',
    port: 5432,
    user: 'brad',
    database: 'brad',
  });

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
