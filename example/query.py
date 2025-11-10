from federated_query.config import load_config
from federated_query.catalog import Catalog
from federated_query.parser import Parser, Binder
from federated_query.optimizer import RuleBasedOptimizer
from federated_query.executor import Executor
import duckdb
import pyarrow as pa
from federated_query.catalog import Catalog
from federated_query.catalog.schema import Schema, Table, Column, DataType
from federated_query.datasources.duckdb import DuckDBDataSource
from federated_query.parser import Parser, Binder
from federated_query.optimizer.physical_planner import PhysicalPlanner
from federated_query.executor.executor import Executor
from federated_query.config.config import ExecutorConfig

def main():
    # Load configuration
    config = load_config("dbconfig.yaml")

    # TODO: how do i get catalog from config
    parser = Parser()
    binder = Binder(catalog)
    planner = PhysicalPlanner(catalog)
    executor = Executor(ExecutorConfig())

    sql = """
        SELECT o.id, c.name, o.amount
        FROM postgres_prod.public.orders o
        JOIN local_duckdb.main.customers c ON o.customer_id = c.id
        WHERE o.amount > 1000
    """

    ast = parser.parse(sql)
    logical_plan = parser.ast_to_logical_plan(ast)
    bound_plan = binder.bind(logical_plan)
    physical_plan = planner.plan(bound_plan)

    result_table = executor.execute_to_table(physical_plan)

    print(result_table)

if __name__ == '__main__':
    main()