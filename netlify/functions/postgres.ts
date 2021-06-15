import { Handler } from "@netlify/functions";
import { Request, Server } from "./protocol";
import { Connection } from 'postgresql-client';

const server : Server = {
  test_connection: async (request) => {
    const connection = new Connection({
      host: 'localhost',
      port: 5432,
      user: 'brad',
      database: 'brad',
    });

    await connection.connect();

    const result = await connection.query('select 1 as one');
    const rows = result.rows;
    await connection.close();

    return { success: true };
  },
};

const handler: Handler = async (event, context) => {
  const request = JSON.parse(event.body);

  const method = request.method;
  const result = await server[method](request.params);
  const response = {
    jsonrpc: "2.0",
    id: request.id,
    result,
  }

  return {
    statusCode: 200,
    body: JSON.stringify(response),
  };
};

export { handler };
