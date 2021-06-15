import { Handler } from "@netlify/functions";
import { Request, Server } from "./protocol";
import { Connection } from 'postgresql-client';

const server = (connection) => {
  return {
    test_connection: async (request) => {
      const result = await connection.query('select 1 as one');
      const rows = result.rows;
      return { success: true };
    },

    list_objects: async (request) => {
      const result = await connection.query("select table_schema, table_name from information_schema.tables where table_schema NOT IN ('pg_catalog', 'information_schema')");
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
