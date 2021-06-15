export type Operation = 'insert' | 'update' | 'upsert';

export type Identifier = number | string;
export type Scalar = number | string | boolean | null;
export type DataValue = Scalar | Scalar[];

type SuccessOrError = {
  success: boolean,
  error_message?: string,
}

export type RecordResult = { identifier: Identifier } & SuccessOrError;
export type FieldType = 'boolean' | 'float' | 'integer' | 'string' | 'date' | 'date_time' | 'decimal';

export type ObjectApiName = string;
export type FieldApiName = string;


export type Object = {
  object_api_name: ObjectApiName,
  label: string,
}

export type Field = {
  field_api_name: FieldApiName,
  label: string,
  identifier: boolean,
  required: boolean,
  createable: boolean,
  updateable: boolean,
  array: boolean,
  type: FieldType,
};

export type SyncPlan = {
  object: Object,
  operation: Operation,
  schema: {
    [field_api_name: string]: {active_identifier: boolean, field: Field},
  },
}

export namespace Request {
  export type TestConnection = void;

  export type ListObjects = void;

  export type ListFields = {
    object: Object,
  }

  export type SupportedOperations = {
    object: Object,
  }

  export type GetSyncSpeed = {
    sync_plan: SyncPlan,
  }

  export type SyncBatch = {
    sync_plan: SyncPlan,
    records: {
      [field_api_name: string]: DataValue
    }[]
  }
}

export namespace Response {
  export type TestConnection = SuccessOrError;

  // TODO would be great to have object groups here
  export type ListObjects = {
    objects: Object[],
  };

  export type ListFields = {
    fields: Field[],
  };

  export type SupportedOperations = {
    operations: Operation[],
  };

  export type GetSyncSpeed = {
    maximum_batch_size: number,
    maximum_records_per_second: number,
    maximum_parallel_batches: number,
  }

  export type SyncBatch = {
    record_results: RecordResult[],
  };
}

export type Server = {
  test_connection     : (request: Request.TestConnection     ) => Promise<Response.TestConnection     >,
  list_objects        : (request: Request.ListObjects        ) => Promise<Response.ListObjects        >,
  list_fields         : (request: Request.ListFields         ) => Promise<Response.ListFields         >,
  supported_operations: (request: Request.SupportedOperations) => Promise<Response.SupportedOperations>,
  get_sync_speed      : (request: Request.GetSyncSpeed       ) => Promise<Response.GetSyncSpeed       >,
  sync_batch          : (request: Request.SyncBatch          ) => Promise<Response.SyncBatch          >,
}
