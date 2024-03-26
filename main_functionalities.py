from colorama import Fore
from promg import OcedPg, SemanticHeader, DatasetDescriptions
from promg.modules.db_management import DBManagement
from promg.modules.task_identification import TaskIdentification

from custom_modules.custom_modules.delay_analysis import PerformanceAnalyzeDelays
from custom_modules.custom_modules.df_interactions import InferDFInteractions
from custom_modules.custom_modules.discover_dfg import DiscoverDFG


def clear_db(db_connection):
    print(Fore.RED + 'Clearing the database.' + Fore.RESET)

    db_manager = DBManagement(db_connection)
    db_manager.clear_db(replace=True)
    db_manager.set_constraints()


def load_data(db_connection, config):
    if config.use_preprocessed_files:
        print(Fore.RED + '💾 Preloaded files are used!' + Fore.RESET)
    else:
        print(Fore.RED + '📝 Importing and creating files' + Fore.RESET)

    semantic_header = SemanticHeader.create_semantic_header(config=config)
    dataset_descriptions = DatasetDescriptions(config=config)

    oced_pg = OcedPg(database_connection=db_connection,
                     semantic_header=semantic_header,
                     dataset_descriptions=dataset_descriptions,
                     store_files=False,
                     use_sample=config.use_sample,
                     use_preprocessed_files=config.use_preprocessed_files,
                     import_directory=config.import_directory)
    oced_pg.load()


def transform_data(db_connection,
                   config):
    dataset_descriptions = DatasetDescriptions(config=config)
    semantic_header = SemanticHeader.create_semantic_header(config=config)

    oced_pg = OcedPg(database_connection=db_connection,
                     semantic_header=semantic_header,
                     dataset_descriptions=dataset_descriptions,
                     store_files=False,
                     use_sample=config.use_sample,
                     use_preprocessed_files=config.use_preprocessed_files,
                     import_directory=config.import_directory)

    oced_pg.transform()
    oced_pg.create_df_edges()


def delete_parallel_df(db_connection, config):
    semantic_header = SemanticHeader.create_semantic_header(config=config)
    print(Fore.RED + 'Inferring DF over relations between objects.' + Fore.RESET)
    infer_df_interactions = InferDFInteractions(db_connection=db_connection, semantic_header=semantic_header)
    infer_df_interactions.delete_parallel_directly_follows_derived('CASE_AO', 'Application')
    infer_df_interactions.delete_parallel_directly_follows_derived('CASE_AO', 'Offer')
    infer_df_interactions.delete_parallel_directly_follows_derived('CASE_AW', 'Application')
    infer_df_interactions.delete_parallel_directly_follows_derived('CASE_AW', 'Workflow')
    infer_df_interactions.delete_parallel_directly_follows_derived('CASE_WO', 'Workflow')
    infer_df_interactions.delete_parallel_directly_follows_derived('CASE_WO', 'Offer')


def discover_model(db_connection, config):
    semantic_header = SemanticHeader.create_semantic_header(config)
    print(Fore.RED + 'Discovering multi-object DFG.' + Fore.RESET)
    dfg = DiscoverDFG(db_connection=db_connection, semantic_header=semantic_header)
    dfg.discover_dfg_for_entity("Application", 25000, 0.0)
    dfg.discover_dfg_for_entity("Offer", 25000, 0.0)
    dfg.discover_dfg_for_entity("Workflow", 25000, 0.0)
    dfg.discover_dfg_for_entity("CASE_AO", 25000, 0.0)
    dfg.discover_dfg_for_entity("CASE_AW", 25000, 0.0)
    dfg.discover_dfg_for_entity("CASE_WO", 25000, 0.0)


def build_tasks(db_connection, config):
    semantic_header = SemanticHeader.create_semantic_header(config)
    print(Fore.RED + 'Detecting tasks.' + Fore.RESET)
    task_identifier = TaskIdentification(
        db_connection=db_connection,
        semantic_header=semantic_header,
        resource="Resource",
        case="CaseAWO")
    task_identifier.identify_tasks()
    task_identifier.aggregate_on_task_variant()


def infer_delays(db_connection):
    print(Fore.RED + 'Computing delay edges.' + Fore.RESET)
    delays = PerformanceAnalyzeDelays(db_connection)
    # delays.enrich_with_delay_edges()
    # delays.analyze_delays()
    delays.visualize_delays(10000)


def print_statistics(db_connection):
    db_manager = DBManagement(db_connection)
    db_manager.print_statistics()
