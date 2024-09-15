1. **Start**: An event is triggered whenever there is a new entry in DynamoDB.
2. **Event Triggered (DynamoDB Entry)**: This event triggers a Lambda function.
3. **AWS Lambda Function Triggered**: The Lambda function is activated.
4. **Flatten Data in Lambda**: The Lambda function processes the DynamoDB entry, flattening and normalizing the data.
5. **Insert Flattened Data into PostgreSQL Ledger Table**: The processed data is inserted into a PostgreSQL ledger table with specific types (weight, totalCalories, height) and sources (Health connect, FITBIT).
6. **Perform Data Aggregation**: The inserted records are aggregated based on user_id, calculating necessary metrics.
7. **Insert Aggregated Data into Second Table**: The aggregated results are populated into a second table, categorized by user_id.
8. **End**: The process completes.

### Flowchart

```mermaid
graph TD;
    A(Start) --> B(Event Triggered (DynamoDB Entry));
    B --> C(AWS Lambda Function Triggered);
    C --> D(Flatten Data in Lambda);
    D --> E(Insert Flattened Data into PostgreSQL Ledger Table);
    E --> F(Perform Data Aggregation);
    F --> G(Insert Aggregated Data into Second Table);
    G --> H(End);

    A[Start]
    B[Event Triggered (DynamoDB Entry)]
    C[AWS Lambda Function Triggered]
    D[Flatten Data in Lambda]
    E[Insert Flattened Data into PostgreSQL Ledger Table]
    F[Perform Data Aggregation]
    G[Insert Aggregated Data into Second Table]
    H[End]