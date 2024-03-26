from datetime import datetime

from promg import SemanticHeader, OcedPg
from promg import DatabaseConnection
from promg import DatasetDescriptions
from promg import Configuration

from promg import Performance
from promg.modules.db_management import DBManagement
from promg.modules.task_identification import TaskIdentification

from modules.custom_modules.df_interactions import InferDFInteractions
from modules.task_concept_drift_detection import concept_drift_analysis

from colorama import Fore

from analysis_configuration import AnalysisConfiguration

config = Configuration()
analysis_config = AnalysisConfiguration()
semantic_header = SemanticHeader.create_semantic_header(config=config)
dataset_descriptions = DatasetDescriptions(config=config)

step_cluster_task = True

# steps for concept drift detection, each can be switched on/off
step_process_level_drift_detection = True
step_actor_drift_detection = False
step_collab_drift_detection = False

# steps for evaluation, can only be turned is the above steps have run
step_eval_actor_vs_process_drift = False
step_eval_collab_vs_process_drift = False
step_calculate_magnitude = False
step_calculate_change_magnitude_percentiles = False
step_detailed_change_signal_analysis = False


def main() -> None:
    """
    Main function, read all the logs, clear and create the graph, perform checks
    @return: None
    """
    print("Started at =", datetime.now().strftime("%H:%M:%S"))

    db_connection = DatabaseConnection.set_up_connection(config=config)
    performance = Performance.set_up_performance(config=config)
    db_manager = DBManagement()

    if step_cluster_task:
        print(Fore.RED + 'Clustering tasks.' + Fore.RESET)
        # TODO: Add modules for clustering tasks and writing to EKG

    if step_process_level_drift_detection:
        print(Fore.RED + 'Detecting process-level concept drift.' + Fore.RESET)
        # TODO: fix import from config file
        concept_drift_analysis.detect_process_level_drift(dataset_name=analysis_config.dataset_name,
                                                          window_sizes=analysis_config.window_sizes,
                                                          penalties=analysis_config.penalties,
                                                          feature_sets=analysis_config.process_drift_feature_sets,
                                                          analysis_directory=analysis_config.analysis_directory,
                                                          exclude_cluster=analysis_config.leftover_cluster,
                                                          plot_drift=False)

        # actors = eg.query_actor_list(min_freq=500)

        # collab_pairs = eg.query_collab_list(min_freq=300)

        # TODO: Add steps for concept drift detection

    performance.finish_and_save()
    db_manager.print_statistics()

    db_connection.close_connection()

if __name__ == "__main__":
    main()
