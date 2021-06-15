import { Handler } from "@netlify/functions";
import { Request, Server } from "./protocol";

const server : Server = {
  test_connection: async (request) => {
    return { success: true };
  }
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
