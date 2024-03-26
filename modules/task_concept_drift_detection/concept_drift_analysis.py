import pandas as pd
import numpy as np
from tabulate import tabulate
from tqdm import tqdm
from itertools import product
from os import path
import os
import glob

from promg import DatabaseConnection
from promg.data_managers.semantic_header import ConstructedNodes, SemanticHeader

from queries.task_cd_detection import ConceptDriftDetectionTasksQueryLibrary as ql
from modules.task_concept_drift_detection.feature_extraction import FeatureExtraction
from modules.task_concept_drift_detection import change_point_detection
from modules.task_concept_drift_detection import change_point_visualization
from queries import query_result_parser as qp


def get_feature_extractor_objects(dataset_name: str, case: ConstructedNodes, resource: ConstructedNodes,
                                  feature_names: list, window_sizes: list, exclude_cluster: str = ""):
    list_f_extr = []
    for window_size in window_sizes:
        f_extr = FeatureExtraction(dataset_name=dataset_name, case=case, actor=resource,
                                   exclude_cluster=exclude_cluster)
        f_extr.retrieve_subgraphs_for_feature_extraction(window_size=window_size, feature_set=feature_names)
        list_f_extr.append(f_extr)
    return list_f_extr


def strip_inactive_windows(features):
    index_where_inactive = list(np.where(np.all(features == 0, axis=1)))
    original_index = list(range(0, len(features)))
    original_index_stripped = sorted(np.setdiff1d(original_index, index_where_inactive))
    features_stripped = features[np.where(~np.all(features == 0, axis=1))]
    return features_stripped, original_index_stripped, index_where_inactive[0]


def strip_inactive_windows_collab(features, actor_1_activity, actor_2_activity):
    index_where_inactive_actor_1 = list(np.where(np.all(actor_1_activity == 0, axis=1)))
    index_where_inactive_actor_2 = list(np.where(np.all(actor_2_activity == 0, axis=1)))
    list_either_inactive = np.concatenate([index_where_inactive_actor_1, index_where_inactive_actor_2],
                                          axis=1).flatten()
    index_where_either_inactive = sorted(list(set(list_either_inactive)))
    original_index = list(range(0, len(features)))
    original_index_stripped = sorted(np.setdiff1d(original_index, index_where_either_inactive))
    features_stripped = np.delete(features, index_where_either_inactive, 0)
    return features_stripped, original_index_stripped, index_where_either_inactive


def retrieve_original_cps(cps, original_index_stripped):
    actual_cps = []
    for cp in cps:
        actual_cps.append(original_index_stripped[cp])
    return actual_cps


def remove_duplicate_collab_pairs(collab_pairs):
    collab_pairs_distinct = []
    for collab_pair in collab_pairs:
        sorted_collab_pair = sorted(collab_pair)
        if sorted_collab_pair not in collab_pairs_distinct:
            collab_pairs_distinct.append(sorted_collab_pair)
    return collab_pairs_distinct


def split_consecutive(array, step_size=1):
    if np.any(array):
        idx = np.r_[0, np.where(np.diff(array) != step_size)[0] + 1, len(array)]
        list_consecutive = [array[i:j] for i, j in zip(idx, idx[1:])]
        return list_consecutive
    else:
        return None


class ConceptDriftDetection:
    def __init__(self, dataset_name, resource: str, case: str):
        self.connection = DatabaseConnection()
        self.dataset_name = dataset_name
        self.output_directory = f"output_final\\{dataset_name}\\task_concept_drift_detection"
        self.resource: ConstructedNodes = SemanticHeader().get_entity(resource)
        self.case: ConstructedNodes = SemanticHeader().get_entity(case)

    def detect_process_level_drift(self, window_sizes, penalties, feature_sets, exclude_cluster,
                                   plot_drift=False):
        all_features = [item for sublist in list(feature_sets.values()) for item in sublist]
        list_f_extr = get_feature_extractor_objects(dataset_name=self.dataset_name, case=self.case,
                                                    resource=self.resource, feature_names=all_features,
                                                    window_sizes=window_sizes, exclude_cluster=exclude_cluster)

        # create output directory for process drift detection
        process_level_drift_directory = os.path.join(self.output_directory, "process_level_drift")
        os.makedirs(process_level_drift_directory, exist_ok=True)

        # initialize dataframe to store change points
        cp_settings = ["{}_{}".format(a_, b_) for a_, b_ in product(window_sizes, penalties)]
        indices = [fs_name for fs_name, _ in feature_sets.items()]
        df_process_level_drift_points = pd.DataFrame(index=indices, columns=cp_settings)
        if path.exists(f"{process_level_drift_directory}\\cps_all_features.csv"):
            df_process_level_drift_points_old = pd.read_csv(f"{process_level_drift_directory}\\cps_all_features.csv",
                                                            index_col=0)
            df_process_level_drift_points = pd.concat(
                [df_process_level_drift_points, df_process_level_drift_points_old],
                ignore_index=False).dropna(how='all').sort_index(axis=1)

        for feature_set_name, feature_list in feature_sets.items():
            df_process_level_feature_drift_points = pd.DataFrame(index=[feature_set_name], columns=cp_settings)
            if path.exists(f"{process_level_drift_directory}\\{feature_set_name}\\cps_{feature_set_name}.csv"):
                df_process_level_feature_drift_points_old = pd.read_csv(
                    f"{process_level_drift_directory}\\{feature_set_name}\\cps_{feature_set_name}.csv", index_col=0)
                df_process_level_feature_drift_points = pd.concat(
                    [df_process_level_feature_drift_points, df_process_level_feature_drift_points_old],
                    ignore_index=False).dropna(how='all').sort_index(axis=1)

            # create output directory for specified feature set
            process_level_drift_feature_directory = os.path.join(process_level_drift_directory, feature_set_name)
            os.makedirs(process_level_drift_feature_directory, exist_ok=True)

            # initialize dictionary to store change points for specified feature set
            dict_process_cps = {}
            for index, window_size in enumerate(window_sizes):
                feature_names, feature_vector = list_f_extr[index].apply_feature_extraction(feature_list)
                reduced_feature_vector = list_f_extr[index].pca_reduction(feature_vector, 'mle', normalize=True,
                                                                          normalize_function="max")
                for pen in penalties:
                    # detect change points for specified penalty and write to dataframe
                    cp = change_point_detection.rpt_pelt(reduced_feature_vector, pen=pen)
                    dict_process_cps[pen] = cp
                    # print(f"Change points {feature_set_name}: {cp}")
                    df_process_level_drift_points.loc[feature_set_name, f"{window_size}_{pen}"] = str(cp)
                    df_process_level_feature_drift_points.loc[feature_set_name, f"{window_size}_{pen}"] = str(cp)
                if plot_drift:
                    change_point_visualization.plot_trends(np_feature_vectors=feature_vector,
                                                           feature_names=feature_names, window_size=window_size,
                                                           output_directory=process_level_drift_feature_directory,
                                                           dict_change_points=dict_process_cps, min_freq=5)
            df_process_level_feature_drift_points.to_csv(
                f"{process_level_drift_directory}\\{feature_set_name}\\cps_{feature_set_name}.csv")
        df_process_level_drift_points.to_csv(f"{process_level_drift_directory}\\cps_all_features.csv")

    def detect_actor_drift(self, window_sizes, penalties, feature_sets, min_actor_freq, exclude_cluster,
                           plot_drift=False):
        actor_list = qp.parse_to_list(self.connection.exec_query(ql.q_retrieve_actor_list, **{
            "resource": self.resource, "min_freq": min_actor_freq}), "actor")

        all_features = [item for sublist in list(feature_sets.values()) for item in sublist]
        list_f_extr = get_feature_extractor_objects(self.dataset_name, self.case, self.resource, all_features,
                                                    window_sizes, exclude_cluster)

        for feature_set_name, feature_list in feature_sets.items():
            print(f"Feature set: {feature_set_name}")

            # create output directory for (actor drift detection X specified feature set)
            actor_drift_feature_directory = os.path.join(self.output_directory, f"actor_drift\\{feature_set_name}")
            os.makedirs(actor_drift_feature_directory, exist_ok=True)

            # initialize dataframe to store change points for specified feature set
            cp_settings = ["{}_{}".format(a_, b_) for a_, b_ in product(window_sizes, penalties)]
            df_all_actor_drift_points = pd.DataFrame(index=actor_list, columns=cp_settings)
            if path.exists(f"{actor_drift_feature_directory}\\all_actor_cps_{feature_set_name}.csv"):
                df_all_actor_drift_points_old = pd.read_csv(
                    f"{actor_drift_feature_directory}\\all_actor_cps_{feature_set_name}.csv",
                    index_col=0)
                df_all_actor_drift_points = pd.concat(
                    [df_all_actor_drift_points, df_all_actor_drift_points_old], ignore_index=False).dropna(
                    how='all').sort_index(axis=1)

            print(f"Detecting change points for {len(actor_list)} actors...")
            for actor in tqdm(actor_list):
                # create analysis directory for actor to plot
                if plot_drift:
                    actor_drift_feature_subdirectory = os.path.join(actor_drift_feature_directory, actor)
                    os.makedirs(actor_drift_feature_subdirectory, exist_ok=True)
                df_actor_drift_points = pd.DataFrame(index=[actor], columns=cp_settings)
                if path.exists(f"{actor_drift_feature_directory}\\{actor}\\{actor}_cps_{feature_set_name}.csv"):
                    df_actor_drift_points_old = pd.read_csv(
                        f"{actor_drift_feature_directory}\\{actor}\\{actor}_cps_{feature_set_name}.csv",
                        index_col=0)
                    df_actor_drift_points = pd.concat(
                        [df_actor_drift_points, df_actor_drift_points_old], ignore_index=False).dropna(
                        how='all').sort_index(axis=1)

                # initialize dictionary to store change points for specified feature set
                dict_actor_cps = {}
                for index, window_size in enumerate(window_sizes):
                    # generate mv time series for specified features/actor/window size
                    actor_feature_names, actor_feature_vector = list_f_extr[index].apply_feature_extraction(
                        feature_list, actor=actor, actor_1=actor, actor_2=actor)
                    actor_feature_vector_stripped, time_window_mapping, windows_inactive = strip_inactive_windows(
                        actor_feature_vector)
                    reduced_actor_feature_vector = list_f_extr[index].pca_reduction(actor_feature_vector_stripped,
                                                                                    'mle',
                                                                                    normalize=True,
                                                                                    normalize_function="max")
                    for pen in penalties:
                        # detect change points for specified penalty and write to dataframe
                        cp = change_point_detection.rpt_pelt(reduced_actor_feature_vector, pen=pen)
                        # print(f"Change points {actor} {feature_set[1]} (pen={pen}): {cp}")
                        cp = retrieve_original_cps(cp, time_window_mapping)
                        df_all_actor_drift_points.loc[actor, f"{window_size}_{pen}"] = str(cp)
                        df_actor_drift_points.loc[actor, f"{window_size}_{pen}"] = str(cp)
                        dict_actor_cps[pen] = cp
                    if plot_drift:
                        change_point_visualization.plot_trends(
                            np_feature_vectors=actor_feature_vector, feature_names=actor_feature_names,
                            window_size=window_size, output_directory=actor_drift_feature_subdirectory,
                            dict_change_points=dict_actor_cps, subgroup=actor, min_freq=5,
                            highlight_ranges=split_consecutive(windows_inactive))

                # df_actor_drift_points.to_csv(
                #     f"{actor_drift_feature_directory}\\{actor}\\{actor}_cps_{feature_set_name}.csv")
            df_all_actor_drift_points.to_csv(f"{actor_drift_feature_directory}\\all_actor_cps_{feature_set_name}.csv")

    def detect_collab_drift(self, window_sizes, penalties, min_collab_freq, detailed_analysis,
                            exclude_cluster, plot_drift=False):
        collab_list = qp.parse_to_2d_list(self.connection.exec_query(ql.q_retrieve_collab_list, **{
            "resource": self.resource, "case": self.case, "min_freq": min_collab_freq}),
                                          "actor_1", "actor_2")
        collab_pairs_distinct = remove_duplicate_collab_pairs(collab_list)
        if detailed_analysis:
            feature_sets = {"task_handovers_case": ["count_per_task_handover_case"]}
        else:
            feature_sets = {"total_task_handovers_case": ["total_task_handover_count_case"]}

        all_features = [item for sublist in list(feature_sets.values()) for item in sublist]
        all_features += ["total_task_count"]
        list_f_extr = get_feature_extractor_objects(self.dataset_name, self.case, self.resource, all_features,
                                                    window_sizes, exclude_cluster)

        # set up indices and columns for dataframes
        cp_settings = ["{}_{}".format(a_, b_) for a_, b_ in product(window_sizes, penalties)]
        index_collab_pairs = [f"{pair[0]}_{pair[1]}" for pair in collab_pairs_distinct]

        # retrieve activity per time window for all actors in collab_pairs
        list_actors = [actor for collab_pair in collab_pairs_distinct for actor in collab_pair]
        dicts_actor_activity_per_ws = []
        for i, ws in enumerate(window_sizes):
            dict_actor_activity = {}
            for a in list_actors:
                _, actor_activity = list_f_extr[i].apply_feature_extraction(["total_task_count"], actor=a)
                dict_actor_activity[a] = actor_activity
            dicts_actor_activity_per_ws.append(dict_actor_activity)

        for feature_set_name, feature_list in feature_sets.items():
            print(f"Feature set: {feature_set_name}")

            # create analysis directory for (collab drift detection X specified feature set)
            collab_drift_feature_directory = os.path.join(self.output_directory, f"collab_drift\\{feature_set_name}")
            os.makedirs(collab_drift_feature_directory, exist_ok=True)

            # initialize dataframe to store change points for specified feature set
            df_all_collab_drift_points = pd.DataFrame(index=index_collab_pairs, columns=cp_settings)
            if path.exists(f"{collab_drift_feature_directory}\\collab_cp_{feature_set_name}.csv"):
                df_all_collab_drift_points_old = pd.read_csv(
                    f"{collab_drift_feature_directory}\\collab_cp_{feature_set_name}.csv",
                    index_col=0)
                df_all_collab_drift_points = pd.concat(
                    [df_all_collab_drift_points, df_all_collab_drift_points_old], ignore_index=False).dropna(
                    how='all').sort_index(axis=1)

            for collab_pair in tqdm(collab_pairs_distinct):
                # create analysis directory for actor to plot
                if plot_drift:
                    collab_drift_feature_subdirectory = os.path.join(collab_drift_feature_directory,
                                                                     f"{collab_pair[0]}_{collab_pair[1]}")
                    os.makedirs(collab_drift_feature_subdirectory, exist_ok=True)

                collab_pair_reversed = collab_pair[::-1]
                df_collab_drift_points = pd.DataFrame(index=[f"{collab_pair[0]}_{collab_pair[1]}"], columns=cp_settings)
                if path.exists(
                        f"{collab_drift_feature_directory}\\{collab_pair[0]}_{collab_pair[1]}\\{collab_pair[0]}_{collab_pair[1]}_cps_{feature_set_name}.csv"):
                    df_collab_drift_points_old = pd.read_csv(
                        f"{collab_drift_feature_directory}\\{collab_pair[0]}_{collab_pair[1]}\\{collab_pair[0]}_{collab_pair[1]}_cps_{feature_set_name}.csv",
                        index_col=0)
                    df_collab_drift_points = pd.concat(
                        [df_collab_drift_points, df_collab_drift_points_old], ignore_index=False).dropna(
                        how='all').sort_index(axis=1)

                dict_collab_cps = {}
                for index, window_size in enumerate(window_sizes):
                    actor_1_activity = dicts_actor_activity_per_ws[index][collab_pair[0]]
                    actor_2_activity = dicts_actor_activity_per_ws[index][collab_pair[1]]

                    # generate mv time series for specified features/collab_pair/window size
                    collab_feature_names, collab_feature_vector = list_f_extr[index].apply_feature_extraction(
                        feature_list, actor_1=collab_pair[0], actor_2=collab_pair[1])
                    collab_feature_names = [f"dir1_{f_name}" for f_name in collab_feature_names]
                    collab_reverse_feature_names, collab_reverse_feature_vector = list_f_extr[
                        index].apply_feature_extraction(
                        feature_list, actor_1=collab_pair_reversed[0], actor_2=collab_pair_reversed[1])
                    collab_reverse_feature_names = [f"dir2_{f_name}" for f_name in collab_reverse_feature_names]
                    collab_total_feature_vector = np.concatenate((collab_feature_vector, collab_reverse_feature_vector),
                                                                 axis=1)
                    collab_total_feature_vector_stripped, time_window_mapping, windows_inactive = \
                        strip_inactive_windows_collab(collab_total_feature_vector, actor_1_activity, actor_2_activity)

                    collab_all_feature_names = collab_feature_names + collab_reverse_feature_names
                    reduced_collab_feature_vector = list_f_extr[index].pca_reduction(
                        collab_total_feature_vector_stripped,
                        'mle', normalize=True,
                        normalize_function="max")
                    for pen in penalties:
                        # detect change points for specified penalty and write to dataframe
                        cp = change_point_detection.rpt_pelt(reduced_collab_feature_vector, pen=pen)
                        cp = retrieve_original_cps(cp, time_window_mapping)
                        dict_collab_cps[pen] = cp
                        print(
                            f"Change points {collab_pair[0]}_{collab_pair[1]} {feature_set_name} (pen={pen}): {cp}")
                        df_all_collab_drift_points.loc[
                            f"{collab_pair[0]}_{collab_pair[1]}", f"{window_size}_{pen}"] = str(
                            cp)
                        df_collab_drift_points.loc[f"{collab_pair[0]}_{collab_pair[1]}", f"{window_size}_{pen}"] = str(
                            cp)
                    if plot_drift:
                        change_point_visualization.plot_trends(
                            np_feature_vectors=collab_total_feature_vector, feature_names=collab_all_feature_names,
                            window_size=window_size, output_directory=collab_drift_feature_subdirectory,
                            dict_change_points=dict_collab_cps, subgroup=f"{collab_pair[0]}_{collab_pair[1]}",
                            min_freq=10,
                            highlight_ranges=split_consecutive(windows_inactive))
                # df_collab_drift_points.to_csv(
                #     f"{collab_drift_feature_directory}\\{collab_pair[0]}_{collab_pair[1]}\\{collab_pair[0]}_{collab_pair[1]}_cps_{feature_set_name}.csv")
            df_all_collab_drift_points.to_csv(f"{collab_drift_feature_directory}\\collab_cp_{feature_set_name}.csv")

    def eval_subgroup_vs_process_drift(self, window_size, pen_process, pen_subgroup, subgroup_type,
                                       feature_set_name_subgroup,
                                       feature_set_name_process_level, output_directory):
        margin = 5
        margin_range = list(range(-margin, margin + 1))
        column_process = f"{window_size}_{pen_process}"
        column_subgroup = f"{window_size}_{pen_subgroup}"
        process_drift_feature_directory = os.path.join(output_directory,
                                                       f"process_level_drift\\{feature_set_name_process_level}")
        df_process_level_drift = pd.read_csv(
            f"{process_drift_feature_directory}\\cps_{feature_set_name_process_level}.csv",
            index_col=0,
            converters={column_process: lambda x: [] if x == "[]" else [int(y) for y in
                                                                        x.strip(
                                                                            "[]").split(
                                                                            ", ")]})
        process_level_cps = df_process_level_drift.loc[feature_set_name_process_level, column_process]
        subgroup_drift_feature_directory = os.path.join(output_directory,
                                                        f"{subgroup_type}_drift\\{feature_set_name_subgroup}")
        dict_subgroup_level_cps = {}
        for csv_file in glob.glob(
                f"{subgroup_drift_feature_directory}\\User_*\\User_*_cps_{feature_set_name_subgroup}.csv"):
            df_subgroup_drift = pd.read_csv(csv_file, index_col=0, converters={
                column_subgroup: lambda x: [] if x == "[]" else [int(y) for y in x.strip("[]").split(", ")]})
            subgroup = df_subgroup_drift.index.tolist()[0]
            subgroup_cps = df_subgroup_drift.loc[subgroup, column_subgroup]
            dict_subgroup_level_cps[subgroup] = subgroup_cps

        dict_cps_subgroups = {v: k for k, l in dict_subgroup_level_cps.items() for v in l}

        lines_to_write = []

        for pcp in process_level_cps:
            line = f"Process level cp = {pcp}"
            print(line)
            lines_to_write.append(line)
            for margin in margin_range:
                subgroups = [key for key, value in dict_subgroup_level_cps.items() if (pcp + margin) in value]
                line = f"\t{subgroup_type}s with cp at {pcp} {margin}: {subgroups}"
                print(line)
                lines_to_write.append(line)
                dict_cps_subgroups.pop((pcp + margin), None)

        line = f"Non process level change points:"
        print(line)
        lines_to_write.append(line)
        dict_cps_subgroups = dict(sorted(dict_cps_subgroups.items(), reverse=True))
        for cp, subgroups in dict_cps_subgroups.items():
            line = f"\tcp = {cp}, {subgroup_type}s = {subgroups}"
            print(line)
            lines_to_write.append(line)

        eval_subgroup_vs_process_drift_directory = os. \
            path.join(output_directory, f"eval_{subgroup_type}_vs_process_drift")
        os.makedirs(eval_subgroup_vs_process_drift_directory, exist_ok=True)
        with open(
                f"{eval_subgroup_vs_process_drift_directory}\\{feature_set_name_subgroup}_ws{window_size}_p{pen_process}_p{pen_subgroup}.txt",
                'w') as f:
            f.write('\n'.join(lines_to_write))

    def calculate_signal_magnitude(self, window_size, penalty, feature_sets, output_directory, exclude_cluster):
        all_features = [item for sublist in list(feature_sets.values()) for item in sublist]

        f_extr = FeatureExtraction(self.dataset_name, exclude_cluster)
        f_extr.query_subgraphs_for_feature_extraction(window_size, all_features)

        # create analysis directory for signal magnitude calculation
        signal_magnitude_directory = os.path.join(output_directory, "signal_magnitude")
        os.makedirs(signal_magnitude_directory, exist_ok=True)

        for feature_set_name, feature_list in feature_sets.items():
            print(feature_set_name)
            feature_names, feature_vector = f_extr.apply_feature_extraction(feature_list)
            np_feature_vectors = np.transpose(feature_vector)
            last_window = np.shape(np_feature_vectors)[1]

            # detect change points for specified penalty and save to list
            reduced_feature_vector = f_extr.pca_reduction(feature_vector, 'mle', normalize=True,
                                                          normalize_function="max")
            cps = change_point_detection.rpt_pelt(reduced_feature_vector, pen=penalty)

            cps.append(0)
            cps.append(last_window)
            cps = sorted(cps)

            # set up dataframe to store measurements
            intervals = [f"{cps[i]}_{cps[i + 1]}" for i in range(0, len(cps) - 1)]
            interval_comparison = [f"{intervals[i]}_vs_{intervals[i + 1]}" for i in range(0, len(intervals) - 1)]
            intervals_all = intervals + interval_comparison
            measures = ["min", "q1", "q2", "q3", "max", "mean"]
            column_tuples = [(interval, measure) for interval, measure in product(intervals_all, measures)]
            column_index = pd.MultiIndex.from_tuples(column_tuples, names=["interval", "measure"])
            df_signal_magnitudes_feature = pd.DataFrame(index=feature_names, columns=column_index)

            # measure per interval
            for i, feature in enumerate(feature_names):
                print(feature)
                feature_vector = np_feature_vectors[i]
                for j, _ in enumerate(cps[:-1]):
                    interval_name = f"{cps[j]}_{cps[j + 1]}"
                    signal_interval = feature_vector[cps[j]: cps[j + 1]]
                    print(f"Interval: {interval_name} - Signal: {signal_interval}")
                    q1 = np.quantile(signal_interval, .25)
                    q2 = np.quantile(signal_interval, .50)
                    q3 = np.quantile(signal_interval, .75)
                    mean = np.mean(signal_interval)
                    min_value = min(signal_interval)
                    max_value = max(signal_interval)
                    df_signal_magnitudes_feature.loc[feature, (interval_name, "q1")] = q1
                    df_signal_magnitudes_feature.loc[feature, (interval_name, "q2")] = q2
                    df_signal_magnitudes_feature.loc[feature, (interval_name, "q3")] = q3
                    df_signal_magnitudes_feature.loc[feature, (interval_name, "min")] = min_value
                    df_signal_magnitudes_feature.loc[feature, (interval_name, "max")] = max_value
                    df_signal_magnitudes_feature.loc[feature, (interval_name, "mean")] = mean
                    print(
                        f"min: {min_value} \t\tq1: {q1} \t\tq2: {q2} \t\tq3: {q3} \t\tmax: {max_value} \t\tavg: {mean}")

            # compare across intervals
            for pair in interval_comparison:
                first, second = pair.split('_vs_')
                df_signal_magnitudes_feature[(pair, "q1")] = df_signal_magnitudes_feature[(second, "q1")] - \
                                                             df_signal_magnitudes_feature[(first, "q1")]
                df_signal_magnitudes_feature[(pair, "q2")] = df_signal_magnitudes_feature[(second, "q2")] - \
                                                             df_signal_magnitudes_feature[(first, "q2")]
                df_signal_magnitudes_feature[(pair, "q3")] = df_signal_magnitudes_feature[(second, "q3")] - \
                                                             df_signal_magnitudes_feature[(first, "q3")]
                df_signal_magnitudes_feature[(pair, "mean")] = df_signal_magnitudes_feature[(second, "mean")] - \
                                                               df_signal_magnitudes_feature[(first, "mean")]
                df_signal_magnitudes_feature[(pair, "min")] = df_signal_magnitudes_feature[(second, "min")] - \
                                                              df_signal_magnitudes_feature[(first, "min")]
                df_signal_magnitudes_feature[(pair, "max")] = df_signal_magnitudes_feature[(second, "max")] - \
                                                              df_signal_magnitudes_feature[(first, "max")]

            df_signal_magnitudes_feature.to_csv(
                f"{signal_magnitude_directory}\\signal_magnitudes_{penalty}_{feature_set_name}.csv")

    def calculate_change_magnitude_percentiles(self, window_size, penalty, feature_sets, output_directory,
                                               exclude_cluster):
        all_features = [item for sublist in list(feature_sets.values()) for item in sublist]

        f_extr = FeatureExtraction(self.dataset_name, exclude_cluster)
        f_extr.query_subgraphs_for_feature_extraction(window_size, all_features)

        # create analysis directory for signal magnitude calculation
        signal_magnitude_directory = os.path.join(output_directory, "signal_magnitude")
        os.makedirs(signal_magnitude_directory, exist_ok=True)

        index = ["mean", "max"]
        # index = ["P.25", "P.50", "P.60", "P.75", "P.90", "max", "1<M<5", "5<M<10", "10<M"]
        columns = pd.MultiIndex(levels=[[], []], codes=[[], []], names=['feature', 'change_point'])

        df_change_magnitude_percentiles = pd.DataFrame(index=index, columns=columns)

        for feature_set_name, feature_list in feature_sets.items():
            print(feature_set_name)
            feature_names, feature_vector = f_extr.apply_feature_extraction(feature_list)
            np_feature_vectors = np.transpose(feature_vector)
            last_window = np.shape(np_feature_vectors)[1]

            # detect change points for specified penalty and save to list
            reduced_feature_vector = f_extr.pca_reduction(feature_vector, 'mle', normalize=True,
                                                          normalize_function="max")
            cps = change_point_detection.rpt_pelt(reduced_feature_vector, pen=penalty)

            cps.append(0)
            cps.append(last_window)
            cps = sorted(cps)

            # set up dataframe to store measurements
            intervals = [f"{cps[i]}_{cps[i + 1]}" for i in range(0, len(cps) - 1)]
            interval_comparison = [f"{intervals[i]}_vs_{intervals[i + 1]}" for i in range(0, len(intervals) - 1)]

            df_signal_magnitudes_feature = pd.read_csv(
                f"{signal_magnitude_directory}\\signal_magnitudes_{penalty}_{feature_set_name}.csv", index_col=0,
                header=[0, 1])

            for change_point in interval_comparison:
                signal_changes = df_signal_magnitudes_feature[(change_point, "mean")].tolist()
                signal_changes_absolute = [abs(change) for change in signal_changes]
                df_change_magnitude_percentiles.loc["max", (feature_set_name, change_point)] = max(
                    signal_changes_absolute)
                df_change_magnitude_percentiles.loc["mean", (feature_set_name, change_point)] = np.mean(
                    signal_changes_absolute)

        df_change_magnitude_percentiles.to_csv(
            f"{signal_magnitude_directory}\\signal_change_magnitude_avg_mean_{penalty}.csv")

    def compare_tasks_vs_activity_actvity_pair(self, penalty, output_directory, cp_task_dict):
        signal_magnitude_directory = os.path.join(output_directory, "signal_magnitude")
        cp_dic_act = {1: "0_11_vs_11_198", 2: "11_198_vs_198_327", 3: "198_327_vs_327_365", 4: "327_365_vs_365_397"}
        cp_dic_act_df = {1: "0_11_vs_11_198", 2: "11_198_vs_198_329", 3: "198_329_vs_329_366", 4: "329_366_vs_366_397"}
        cp_dic_variant = {1: "0_9_vs_9_196", 2: "9_196_vs_196_330", 3: "196_330_vs_330_368", 4: "330_368_vs_368_397"}
        cp_dic_task = {1: "0_11_vs_11_200", 2: "11_200_vs_200_328", 3: "200_328_vs_328_366", 4: "328_366_vs_366_397"}

        df_task_change_magnitudes = pd.read_csv(
            f"{signal_magnitude_directory}\\signal_magnitudes_{penalty}_task_relative.csv", index_col=0, header=[0, 1])
        df_activity_change_magnitudes = pd.read_csv(
            f"{signal_magnitude_directory}\\signal_magnitudes_{penalty}_activity_relative.csv", index_col=0,
            header=[0, 1])
        df_df_case_activity_change_magnitudes = pd.read_csv(
            f"{signal_magnitude_directory}\\signal_magnitudes_{penalty}_activity_handover_case_relative.csv",
            index_col=0, header=[0, 1])

        index = pd.MultiIndex(levels=[[], []], codes=[[], []], names=['cp', 'task'])
        column_types = ["act_same", "act_opp", "act_df_same", "act_df_opp"]
        measures = ["fraction", "w_average", "max"]
        column_tuples = [(type, measure) for type, measure in product(column_types, measures)]
        column_index = pd.MultiIndex.from_tuples(column_tuples, names=["type", "measure"])

        df_task_detailed_analysis = pd.DataFrame(index=index, columns=column_index)

        for cp, tasks in cp_task_dict.items():
            for task in tasks:
                task_change = df_task_change_magnitudes.loc[f"task_{task}_relative_freq", (cp_dic_task[cp], "mean")]
                task_change_direction = "rising" if task_change > 0 else "falling"

                variant_list = []
                for record in result:
                    variant_list.append(record['variant'])

                variant_list = event_graph.q_list_variants_in_cluster("cluster", f"\"{task}\"")

                self.connection.exec_query(ql.q_list_variants_in_cluster("cluster", f"\"{task}\""))

                all_activity_df = event_graph.query_activity_pairs_in_cluster("cluster", f"\"{task}\"")
                all_activity = event_graph.query_activities_in_cluster("cluster", f"\"{task}\"")
                act_same_direction = {}
                act_opposite_direction = {}
                for activity in all_activity:
                    activity_change = df_activity_change_magnitudes.loc[
                        f"activity_{activity}_relative_freq", (cp_dic_act[cp], "mean")]
                    activity_change_direction = "rising" if activity_change > 0 else "falling"
                    if abs(activity_change) > 0.1 and activity_change_direction == task_change_direction:
                        act_same_direction[activity] = [activity_change, 0]
                    if abs(activity_change) > 0.1 and activity_change_direction != task_change_direction:
                        act_opposite_direction[activity] = [activity_change, 0]
                act_df_same_direction = {}
                act_df_opposite_direction = {}
                for activity_df in all_activity_df:
                    activity_df_change = df_df_case_activity_change_magnitudes.loc[
                        f"activity_{activity_df[0]}_{activity_df[1]}_relative_freq", (cp_dic_act_df[cp], "mean")]
                    activity_df_change_direction = "rising" if activity_df_change > 0 else "falling"
                    if abs(activity_df_change) > 0.1 and activity_df_change_direction == task_change_direction:
                        act_df_same_direction[f"{activity_df[0]}_{activity_df[1]}"] = [activity_df_change, 0]
                    if abs(activity_df_change) > 0.1 and activity_df_change_direction != task_change_direction:
                        act_df_opposite_direction[f"{activity_df[0]}_{activity_df[1]}"] = [activity_df_change, 0]

                fraction_act_df_same = []
                fraction_act_df_opposite = []
                fraction_act_same = []
                fraction_act_opposite = []
                for variant in variant_list:
                    number_act_same = 0
                    number_act_opposite = 0
                    number_act_df_same = 0
                    number_act_df_opposite = 0
                    for i, _ in enumerate(variant[:-1]):
                        action1 = variant[i]
                        action2 = variant[i + 1]
                        if f"{action1}_{action2}" in act_df_same_direction:
                            number_act_df_same += 1
                            act_df_same_direction[f"{action1}_{action2}"][1] += 1
                        if f"{action1}_{action2}" in act_df_opposite_direction:
                            number_act_df_opposite += 1
                            act_df_opposite_direction[f"{action1}_{action2}"][1] += 1
                    fraction_act_df_same.append(number_act_df_same / len(variant[:-1]))
                    fraction_act_df_opposite.append(number_act_df_opposite / len(variant[:-1]))
                    for activity in variant:
                        if activity in act_same_direction:
                            number_act_same += 1
                            act_same_direction[activity][1] += 1
                        if activity in act_opposite_direction:
                            number_act_opposite += 1
                            act_opposite_direction[activity][1] += 1
                    fraction_act_same.append(number_act_same / len(variant))
                    fraction_act_opposite.append(number_act_opposite / len(variant))
                df_task_detailed_analysis.loc[(cp, task), ("act_same", "fraction")] = sum(fraction_act_same) / len(
                    fraction_act_same)
                df_task_detailed_analysis.loc[(cp, task), ("act_opp", "fraction")] = sum(fraction_act_opposite) / len(
                    fraction_act_opposite)
                df_task_detailed_analysis.loc[(cp, task), ("act_df_same", "fraction")] = sum(
                    fraction_act_df_same) / len(fraction_act_df_same)
                df_task_detailed_analysis.loc[(cp, task), ("act_df_opp", "fraction")] = sum(
                    fraction_act_df_opposite) / len(fraction_act_df_opposite)
                if task_change_direction == "rising":
                    df_task_detailed_analysis.loc[(cp, task), ("act_same", "max")] = max(
                        [value[0] for value in act_same_direction.values()]) if act_same_direction.values() else np.nan
                    df_task_detailed_analysis.loc[(cp, task), ("act_opp", "max")] = min([value[0] for value in
                                                                                         act_opposite_direction.values()]) if act_opposite_direction.values() else np.nan
                    df_task_detailed_analysis.loc[(cp, task), ("act_df_same", "max")] = max([value[0] for value in
                                                                                             act_df_same_direction.values()]) if act_df_same_direction.values() else np.nan
                    df_task_detailed_analysis.loc[(cp, task), ("act_df_opp", "max")] = min([value[0] for value in
                                                                                            act_df_opposite_direction.values()]) if act_df_opposite_direction.values() else np.nan
                else:
                    df_task_detailed_analysis.loc[(cp, task), ("act_same", "max")] = min(
                        [value[0] for value in act_same_direction.values()]) if act_same_direction.values() else np.nan
                    df_task_detailed_analysis.loc[(cp, task), ("act_opp", "max")] = max([value[0] for value in
                                                                                         act_opposite_direction.values()]) if act_opposite_direction.values() else np.nan
                    df_task_detailed_analysis.loc[(cp, task), ("act_df_same", "max")] = min([value[0] for value in
                                                                                             act_df_same_direction.values()]) if act_df_same_direction.values() else np.nan
                    df_task_detailed_analysis.loc[(cp, task), ("act_df_opp", "max")] = max([value[0] for value in
                                                                                            act_df_opposite_direction.values()]) if act_df_opposite_direction.values() else np.nan
                if act_same_direction.values():
                    w_average_act_same = sum(value[0] * value[1] for value in act_same_direction.values()) / sum(
                        [value[1] for value in act_same_direction.values()])
                    df_task_detailed_analysis.loc[(cp, task), ("act_same", "w_average")] = w_average_act_same
                if act_opposite_direction.values():
                    w_average_act_opp = sum(value[0] * value[1] for value in act_opposite_direction.values()) / sum(
                        [value[1] for value in act_opposite_direction.values()])
                    df_task_detailed_analysis.loc[(cp, task), ("act_opp", "w_average")] = w_average_act_opp
                if act_df_same_direction.values():
                    w_average_act_df_same = sum(value[0] * value[1] for value in act_df_same_direction.values()) / sum(
                        [value[1] for value in act_df_same_direction.values()])
                    df_task_detailed_analysis.loc[(cp, task), ("act_df_same", "w_average")] = w_average_act_df_same
                if act_df_opposite_direction.values():
                    w_average_act_df_opp = sum(
                        value[0] * value[1] for value in act_df_opposite_direction.values()) / sum(
                        [value[1] for value in act_df_opposite_direction.values()])
                    df_task_detailed_analysis.loc[(cp, task), ("act_df_opp", "w_average")] = w_average_act_df_opp

        print(tabulate(df_task_detailed_analysis, headers='keys', tablefmt='psql'))
        df_task_detailed_analysis.to_csv(
            f"{signal_magnitude_directory}\\signal_change_tasks_detailed_{penalty}.csv")

    def compare_variant_vs_activity_actvity_pair(self, penalty, output_directory, cp_variant_dict):
        signal_magnitude_directory = os.path.join(output_directory, "signal_magnitude")
        cp_dic_act = {1: "0_11_vs_11_198", 2: "11_198_vs_198_327", 3: "198_327_vs_327_365", 4: "327_365_vs_365_397"}
        cp_dic_act_df = {1: "0_11_vs_11_198", 2: "11_198_vs_198_329", 3: "198_329_vs_329_366", 4: "329_366_vs_366_397"}
        cp_dic_variant = {1: "0_9_vs_9_196", 2: "9_196_vs_196_330", 3: "196_330_vs_330_368", 4: "330_368_vs_368_397"}
        cp_dic_task = {1: "0_11_vs_11_200", 2: "11_200_vs_200_328", 3: "200_328_vs_328_366", 4: "328_366_vs_366_397"}

        df_variant_change_magnitudes = pd.read_csv(
            f"{signal_magnitude_directory}\\signal_magnitudes_{penalty}_task_variant_relative.csv", index_col=0,
            header=[0, 1])
        df_activity_change_magnitudes = pd.read_csv(
            f"{signal_magnitude_directory}\\signal_magnitudes_{penalty}_activity_relative.csv", index_col=0,
            header=[0, 1])
        df_df_case_activity_change_magnitudes = pd.read_csv(
            f"{signal_magnitude_directory}\\signal_magnitudes_{penalty}_activity_handover_case_relative.csv",
            index_col=0, header=[0, 1])

        index = pd.MultiIndex(levels=[[], []], codes=[[], []], names=['cp', 'task'])
        column_types = ["act_same", "act_opp", "act_df_same", "act_df_opp"]
        measures = ["fraction", "w_average", "max"]
        column_tuples = [(type, measure) for type, measure in product(column_types, measures)]
        column_index = pd.MultiIndex.from_tuples(column_tuples, names=["type", "measure"])

        df_task_variant_detailed_analysis = pd.DataFrame(index=index, columns=column_index)

        for cp, task_variants in cp_variant_dict.items():
            for variant in task_variants:
                variant_change = df_variant_change_magnitudes.loc[
                    f"task_variant_{variant}_relative_freq", (cp_dic_variant[cp], "mean")]
                variant_change_direction = "rising" if variant_change > 0 else "falling"
                variant_action_sequence = event_graph.q_list_variants_in_cluster("ID", variant)[0]
                all_activity_df = event_graph.query_activity_pairs_in_cluster("ID", variant)
                all_activity = event_graph.query_activities_in_cluster("ID", variant)
                act_same_direction = {}
                act_opposite_direction = {}
                for activity in all_activity:
                    activity_change = df_activity_change_magnitudes.loc[
                        f"activity_{activity}_relative_freq", (cp_dic_act[cp], "mean")]
                    activity_change_direction = "rising" if activity_change > 0 else "falling"
                    if abs(activity_change) > 0.1 and activity_change_direction == variant_change_direction:
                        act_same_direction[activity] = [activity_change, 0]
                    if abs(activity_change) > 0.1 and activity_change_direction != variant_change_direction:
                        act_opposite_direction[activity] = [activity_change, 0]
                act_df_same_direction = {}
                act_df_opposite_direction = {}
                for activity_df in all_activity_df:
                    activity_df_change = df_df_case_activity_change_magnitudes.loc[
                        f"activity_{activity_df[0]}_{activity_df[1]}_relative_freq", (cp_dic_act_df[cp], "mean")]
                    activity_df_change_direction = "rising" if activity_df_change > 0 else "falling"
                    if abs(activity_df_change) > 0.1 and activity_df_change_direction == variant_change_direction:
                        act_df_same_direction[f"{activity_df[0]}_{activity_df[1]}"] = [activity_df_change, 0]
                    if abs(activity_df_change) > 0.1 and activity_df_change_direction != variant_change_direction:
                        act_df_opposite_direction[f"{activity_df[0]}_{activity_df[1]}"] = [activity_df_change, 0]

                number_act_same = 0
                number_act_opposite = 0
                number_act_df_same = 0
                number_act_df_opposite = 0
                for i, _ in enumerate(variant_action_sequence[:-1]):
                    action1 = variant_action_sequence[i]
                    action2 = variant_action_sequence[i + 1]
                    if f"{action1}_{action2}" in act_df_same_direction:
                        number_act_df_same += 1
                        act_df_same_direction[f"{action1}_{action2}"][1] += 1
                    if f"{action1}_{action2}" in act_df_opposite_direction:
                        number_act_df_opposite += 1
                        act_df_opposite_direction[f"{action1}_{action2}"][1] += 1
                fraction_act_df_same = number_act_df_same / len(variant_action_sequence[:-1])
                fraction_act_df_opposite = number_act_df_opposite / len(variant_action_sequence[:-1])
                for activity in variant_action_sequence:
                    if activity in act_same_direction:
                        number_act_same += 1
                        act_same_direction[activity][1] += 1
                    if activity in act_opposite_direction:
                        number_act_opposite += 1
                        act_opposite_direction[activity][1] += 1
                fraction_act_same = number_act_same / len(variant_action_sequence)
                fraction_act_opposite = number_act_opposite / len(variant_action_sequence)

                df_task_variant_detailed_analysis.loc[(cp, variant), ("act_same", "fraction")] = fraction_act_same
                df_task_variant_detailed_analysis.loc[(cp, variant), ("act_opp", "fraction")] = fraction_act_opposite
                df_task_variant_detailed_analysis.loc[(cp, variant), ("act_df_same", "fraction")] = fraction_act_df_same
                df_task_variant_detailed_analysis.loc[
                    (cp, variant), ("act_df_opp", "fraction")] = fraction_act_df_opposite
                if variant_change_direction == "rising":
                    df_task_variant_detailed_analysis.loc[(cp, variant), ("act_same", "max")] = max(
                        [value[0] for value in act_same_direction.values()]) if act_same_direction.values() else np.nan
                    df_task_variant_detailed_analysis.loc[(cp, variant), ("act_opp", "max")] = min(
                        [value[0] for value in
                         act_opposite_direction.values()]) if act_opposite_direction.values() else np.nan
                    df_task_variant_detailed_analysis.loc[(cp, variant), ("act_df_same", "max")] = max(
                        [value[0] for value in
                         act_df_same_direction.values()]) if act_df_same_direction.values() else np.nan
                    df_task_variant_detailed_analysis.loc[(cp, variant), ("act_df_opp", "max")] = min(
                        [value[0] for value in
                         act_df_opposite_direction.values()]) if act_df_opposite_direction.values() else np.nan
                else:
                    df_task_variant_detailed_analysis.loc[(cp, variant), ("act_same", "max")] = min(
                        [value[0] for value in act_same_direction.values()]) if act_same_direction.values() else np.nan
                    df_task_variant_detailed_analysis.loc[(cp, variant), ("act_opp", "max")] = max(
                        [value[0] for value in
                         act_opposite_direction.values()]) if act_opposite_direction.values() else np.nan
                    df_task_variant_detailed_analysis.loc[(cp, variant), ("act_df_same", "max")] = min(
                        [value[0] for value in
                         act_df_same_direction.values()]) if act_df_same_direction.values() else np.nan
                    df_task_variant_detailed_analysis.loc[(cp, variant), ("act_df_opp", "max")] = max(
                        [value[0] for value in
                         act_df_opposite_direction.values()]) if act_df_opposite_direction.values() else np.nan
                if act_same_direction.values():
                    w_average_act_same = sum(value[0] * value[1] for value in act_same_direction.values()) / sum(
                        [value[1] for value in act_same_direction.values()])
                    df_task_variant_detailed_analysis.loc[(cp, variant), ("act_same", "w_average")] = w_average_act_same
                if act_opposite_direction.values():
                    w_average_act_opp = sum(value[0] * value[1] for value in act_opposite_direction.values()) / sum(
                        [value[1] for value in act_opposite_direction.values()])
                    df_task_variant_detailed_analysis.loc[(cp, variant), ("act_opp", "w_average")] = w_average_act_opp
                if act_df_same_direction.values():
                    w_average_act_df_same = sum(value[0] * value[1] for value in act_df_same_direction.values()) / sum(
                        [value[1] for value in act_df_same_direction.values()])
                    df_task_variant_detailed_analysis.loc[
                        (cp, variant), ("act_df_same", "w_average")] = w_average_act_df_same
                if act_df_opposite_direction.values():
                    w_average_act_df_opp = sum(
                        value[0] * value[1] for value in act_df_opposite_direction.values()) / sum(
                        [value[1] for value in act_df_opposite_direction.values()])
                    df_task_variant_detailed_analysis.loc[
                        (cp, variant), ("act_df_opp", "w_average")] = w_average_act_df_opp

        print(tabulate(df_task_variant_detailed_analysis, headers='keys', tablefmt='psql'))
        df_task_variant_detailed_analysis.to_csv(
            f"{signal_magnitude_directory}\\signal_change_variants_detailed_{penalty}.csv")
