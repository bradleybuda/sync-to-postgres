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
      console.log({query});
      const result = await postgres.query(query);
      console.log(result.rows);

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
