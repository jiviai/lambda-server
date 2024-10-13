import logging
import psycopg2
from psycopg2.extras import execute_values
from decimal import Decimal
import psycopg2.sql as sql

# Initialize logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("Loaded %s", __name__)

from smart_health.utils import remove_duplicates
class PostgresDBHandler:
    def __init__(self, host, database, user, password, schema='public'):
        self.connection_params = {
            'host': host,
            'database': database,
            'user': user,
            'password': password
        }
        self.conn = None
        self.cur = None
        self.schema = schema  # Schema can be specified; default is 'public'

    def connect(self):
        """Establish a connection to the PostgreSQL database."""
        try:
            self.conn = psycopg2.connect(**self.connection_params)
            self.cur = self.conn.cursor()
            logger.info("Database connection established.")
        except psycopg2.Error as e:
            logger.error(f'Error connecting to database: {e}')
            raise

    def disconnect(self):
        """Close the database connection."""
        if self.cur is not None:
            self.cur.close()
        if self.conn is not None:
            self.conn.close()
        logger.info("Database connection closed.")

    def read(self, query, params=None):
        """Execute a read query and return the results."""
        try:
            self.cur.execute(query, params)
            result = self.cur.fetchall()
            logger.info(f"Read {len(result)} records.")
            return result
        except psycopg2.Error as e:
            logger.error(f'Error during read: {e}')
            raise

    def write_many(self, table_name, data_list, columns, conflict_columns=None):
        """
        Insert multiple records into the specified table.
        Validates each record before insertion.
        Parameters:
            - table_name: Name of the table to insert data into.
            - data_list: A list of dictionaries containing the data to be inserted.
            - columns: A list of column names corresponding to the keys in data dictionaries.
            - conflict_columns: A list of column names to handle conflicts (for ON CONFLICT clause).
        """
        if not columns:
            logger.info("No columns specified for insertion.")
            return

        data_values = []
        unique_data_list = remove_duplicates(data_list, conflict_columns)
        for record in unique_data_list:
            try:
                validated_record = self.validate_data(record, columns)
                values = [validated_record.get(col) for col in columns]
                data_values.append(tuple(values))
            except ValueError as e:
                logger.error(f"Validation error: {e}")
                continue  # Skip invalid records

        if not data_values:
            logger.info("No valid records to insert.")
            return

        # Build the INSERT query using psycopg2.sql for safe query construction
        insert_query = sql.SQL("""
            INSERT INTO {table} ({fields})
            VALUES %s
        """).format(
            table=sql.Identifier(table_name),
            fields=sql.SQL(', ').join(map(sql.Identifier, columns))
        )

        if conflict_columns:
            # Exclude conflict_columns from the columns to be updated
            update_columns = [col for col in columns if col not in conflict_columns]

            if update_columns:
                # Build the SET expressions
                set_clause = sql.SQL(', ').join(
                    sql.Composed([
                        sql.Identifier(col),
                        sql.SQL(' = EXCLUDED.'),
                        sql.Identifier(col)
                    ])
                    for col in update_columns
                )

                conflict_clause = sql.SQL(" ON CONFLICT ({conflict_fields}) DO UPDATE SET {set_clause}").format(
                    conflict_fields=sql.SQL(', ').join(map(sql.Identifier, conflict_columns)),
                    set_clause=set_clause
                )
                logger.info(f"Conflict detected on columns {conflict_columns}. Rows will be updated on conflict.")
            else:
                # If no columns to update, DO NOTHING
                conflict_clause = sql.SQL(" ON CONFLICT ({conflict_fields}) DO NOTHING").format(
                    conflict_fields=sql.SQL(', ').join(map(sql.Identifier, conflict_columns))
                )
                logger.info(f"Conflict detected on columns {conflict_columns}. No action will be taken on conflict.")

            insert_query = insert_query + conflict_clause

        try:
            execute_values(self.cur, insert_query.as_string(self.cur), data_values)
            self.conn.commit()
            logger.info(f"Inserted {len(data_values)} records into {table_name}.")
        except psycopg2.IntegrityError as e:
            logger.error(f'Database integrity error: {e}')
            self.conn.rollback()
        except psycopg2.Error as e:
            logger.error(f'Error during bulk write: {e}')
            self.conn.rollback()
            raise

    def validate_data(self, record, required_fields):
        """Validate a single record before insertion."""
        for field in required_fields:
            if field not in record or record[field] is None:
                raise ValueError(f"Missing required field: {field}")
        # Custom validation for 'vital_value'
        if 'vital_value' in required_fields:
            if not isinstance(record['vital_value'], (Decimal, float, int)):
                raise ValueError("vital_value must be a number")
        # Additional validation logic can be added here
        return record