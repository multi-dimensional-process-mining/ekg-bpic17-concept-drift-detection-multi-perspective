from datetime import datetime
from promg import DatabaseConnection
from promg import Configuration

from promg import Performance
from promg.modules.db_management import DBManagement

from main_functionalities import clear_db, load_data, transform_data, delete_parallel_df, discover_model, build_tasks, \
    infer_delays, print_statistics


def main(config,
         step_clear_db=True,
         step_populate_graph=True,
         step_delete_parallel_df=True,
         step_discover_model=True,
         step_build_tasks=True,
         step_infer_delays=True
         ) -> None:
    """
    Main function, read all the logs, clear and create the graph, perform checks
    @return: None
    """
    print("Started at =", datetime.now().strftime("%H:%M:%S"))

    db_connection = DatabaseConnection.set_up_connection(config=config)
    performance = Performance.set_up_performance(config=config)

    if step_clear_db:
        clear_db(db_connection)

    if step_populate_graph:
        load_data(db_connection=db_connection,
                  config=config)
        transform_data(db_connection=db_connection,
                       config=config)

    if step_delete_parallel_df:
        delete_parallel_df(db_connection=db_connection, config=config)

    if step_discover_model:
        discover_model(db_connection=db_connection, config=config)

    if step_build_tasks:
        build_tasks(db_connection=db_connection, config=config)

    if step_infer_delays:
        infer_delays(db_connection=db_connection)

    performance.finish_and_save()
    print_statistics(db_connection)

    db_connection.close_connection()


if __name__ == "__main__":
    main(config=Configuration.init_conf_with_config_file(),
         step_clear_db=True,
         step_populate_graph=True,
         step_delete_parallel_df=True,
         step_discover_model=True,
         step_build_tasks=True,
         step_infer_delays=True)
