import tableauhyperapi as tab_api

class TableauDataSource:
    """
    A class for creating and manipulating Tableau data sources using the Tableau Hyper API.
    """


    def __init__(self, datasource_path):
        """
        Constructor for the TableauDataSource class.

        Parameters:
            datasource_path (str): The path to the Tableau data source file.
        """
        self.datasource_path = datasource_path
        self.connection = None
        self.table_definition = None

    def connect(self):
        """
        Creates a connection to the data source.
        """

        if self.connection is None:
            self.connection = tab_api.Connection(endpoint=tab_api.Endpoint('localhost'),
                                                    dbname=self.datasource_path)
            self.connection.open()

    def create_table(self, table_name, table_definition):

        """
        Creates a new table in the data source.

        Parameters:
            table_name (str): The name of the table to be created.
            table_definition (dict): A dictionary that defines the columns of the table and their data types.

        Returns:
            None
        """
        self.connect()

        # Check if the table definition is empty
        if not table_definition:
            raise ValueError("Table definition cannot be empty.")

        # Check if the table name already exists in the data source
        for table in self.connection.catalog.get_tables():
            if table.name == table_name:
                raise ValueError(f"Table {table_name} already exists in the data source.")

        # Create the table
        schema = tab_api.SchemaDefinition(table_name)
        for column_name, data_type in table_definition.items():
            column = tab_api.TableDefinition.Column(column_name, data_type)
            schema.add_column(column)
        self.table_definition = schema
        self.connection.catalog.create_table(self.table_definition)
    def append_rows(self, table_name, rows):
        """
        Appends rows to an existing table in the data source.

        Parameters:
            table_name (str): The name of the table to append rows to.
            rows (list of tuples): A list of tuples representing the rows to be appended.

        Returns:
            None
        """
        # method appends rows to an existing table in the data source
        # table_name: name of the table to append rows to
        # rows: list of tuples representing the rows to be appended
        self.connect()

        # Check if the specified table exists in the data source
        table = self.connection.catalog.get_table(table_name)
        if table is None:
            raise ValueError(f"Table {table_name} does not exist in the data source.")

        # Check if the number of columns in a row matches the number of columns in the table
        num_columns = len(table.table_definition.columns)
        for row in rows:
            if len(row) != num_columns:
                raise ValueError(f"Number of columns in row {row} does not match the number of columns in the table.")

        # Append the rows to the table, rolling back the transaction if an error occurs
        with self.connection.begin():
            try:
                for row in rows:
                    self.connection.execute_insert(tab_api.TableName(table_name), row)
            except:
                self.connection.rollback()
                raise

    def update_rows(self, table_name, update_query):
        """
        Updates rows in an existing table in the data source.

        Parameters:
            table_name (str): The name of the table to update rows in.
            update_query (str): An SQL query string that specifies the rows to be updated and their new values.

        Returns:
            None
        """
        self.connect()

        # Check if the specified table exists in the data source
        table = self.connection.catalog.get_table(table_name)
        if table is None:
            raise ValueError(f"Table {table_name} does not exist in the data source.")

        # Update the rows in the table, rolling back the transaction if an error occurs
        with self.connection.begin():
            try:
                self.connection.execute_update(tab_api.TableName(table_name), update_query)
            except:
                self.connection.rollback()
                raise

    def delete_rows(self, table_name, delete_query):
        """
        Deletes rows from an existing table in the data source.

        Parameters:
            table_name (str): The name of the table to delete rows from.
            delete_query (str): An SQL query string that specifies the rows to be deleted.

        Returns:
            None
        """
        # method deletes rows from an existing table in the data source
        # table_name: name of the table to delete rows from
        # delete_query: an SQL query string that specifies the rows to be deleted
        self.connect()
        with self.connection.begin():
            self.connection.execute_delete(tab_api.TableName(table_name), delete_query)

    def get_datasource_id(self, datasource_name):
        """
        Returns the data source ID for the Tableau data source with the specified name.

        Parameters:
            datasource_name (str): The name of the Tableau data source.

        Returns:
            str: The data source ID for the Tableau data source.
        """
        self.connect()
        for ds in self.connection.catalog.get_datasources():
            if ds.name == datasource_name:
                return ds.id
        raise ValueError(f"Could not find data source with name {datasource_name}")