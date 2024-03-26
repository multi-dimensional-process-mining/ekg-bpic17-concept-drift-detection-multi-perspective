from datetime import datetime
from promg import DatabaseConnection
from promg import Configuration

from promg import Performance
from promg.modules.db_management import DBManagement
from promg.modules.task_identification import TaskIdentification

from modules.task_identification.task_identification import TaskIdentification as TaskIdentificationLocal
from modules.task_clustering.task_clustering import TaskClustering
from modules.task_clustering.cluster_visualization import ClusterVisualizer
from modules.task_concept_drift_detection.concept_drift_analysis import ConceptDriftDetection

from main_functionalities import clear_db, load_data, transform_data, delete_parallel_df, discover_model, build_tasks, \
    infer_delays, print_statistics
from analysis_configuration import AnalysisConfiguration

config = Configuration()
analysis_config = AnalysisConfiguration()
semantic_header = SemanticHeader.create_semantic_header(config=config)
dataset_descriptions = DatasetDescriptions(config=config)

# several steps of import, each can be switched on/off
step_clear_db = False
step_populate_graph = False
step_build_tasks = False

# steps for task clustering, each can be switched on/off
step_cluster_tasks = False
step_visualize_clusters_in_dot = False

# steps for concept drift detection, each can be switched on/off
step_process_level_drift_detection = False
step_actor_drift_detection = False
step_collab_drift_detection = True

# steps for evaluation, can only be turned is the above steps have run
step_eval_actor_vs_process_drift = False
step_eval_collab_vs_process_drift = False
step_calculate_magnitude = False
step_calculate_change_magnitude_percentiles = False
step_detailed_change_signal_analysis = False

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
        task_identifier_local = TaskIdentificationLocal(resource="Resource", case="CaseAWO")
        task_identifier_local.set_task_id()

    if step_infer_delays:
        infer_delays(db_connection=db_connection)

    if step_cluster_tasks:
        print(Fore.RED + 'Clustering tasks.' + Fore.RESET)
        task_clustering = TaskClustering(dataset_name=analysis_config.dataset_name, resource="Resource", case="CaseAWO",
                                         min_variant_freq=analysis_config.cluster_min_variant_freq)
        task_clustering.encode_and_cluster_specific(min_variant_length=analysis_config.cluster_min_variant_length,
                                                    manual_clusters=analysis_config.manual_clusters,
                                                    num_clusters=analysis_config.num_clusters,
                                                    include_remainder=analysis_config.cluster_include_remainder,
                                                    clustering_description=analysis_config.clustering_description)
        task_clustering.remove_clusters()
        task_clustering.construct_clusters()

    if step_visualize_clusters_in_dot:
        cluster_visualization = ClusterVisualizer(dataset_name=analysis_config.dataset_name)
        cluster_visualization.visualize_clusters()

    if step_process_level_drift_detection or step_actor_drift_detection or step_collab_drift_detection:
        cd_detection = ConceptDriftDetection(dataset_name=analysis_config.dataset_name, resource="Resource",
                                             case="CaseAWO")
        if step_process_level_drift_detection:
            print(Fore.RED + 'Detecting process-level drift.' + Fore.RESET)
            cd_detection.detect_process_level_drift(window_sizes=analysis_config.window_sizes,
                                                    penalties=analysis_config.penalties,
                                                    feature_sets=analysis_config.process_drift_feature_sets,
                                                    exclude_cluster=analysis_config.leftover_cluster,
                                                    plot_drift=False)
        if step_actor_drift_detection:
            print(Fore.RED + 'Detecting actor drift.' + Fore.RESET)
            cd_detection.detect_actor_drift(window_sizes=analysis_config.window_sizes,
                                            penalties=analysis_config.penalties,
                                            feature_sets=analysis_config.actor_drift_feature_sets,
                                            min_actor_freq=500, exclude_cluster=analysis_config.leftover_cluster,
                                            plot_drift=False)
        if step_collab_drift_detection:
            print(Fore.RED + 'Detecting collab drift.' + Fore.RESET)
            cd_detection.detect_collab_drift(window_sizes=analysis_config.window_sizes,
                                             penalties=analysis_config.penalties,
                                             min_collab_freq=300, detailed_analysis=True,
                                             exclude_cluster=analysis_config.leftover_cluster, plot_drift=False)

        # TODO: Add steps for concept drift detection


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
