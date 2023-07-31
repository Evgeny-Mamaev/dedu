import json
import random
import unittest
from functools import reduce

import redis
from forbiddenfruit import curse
from unittest.mock import Mock

from fuzzywuzzy import fuzz
from sortedcontainers import SortedDict, SortedSet

import deduplicator
import redisadapter
from deduplicator import dfs_snapshots_to_reprocess, AUX_PARAMS, break_up_master, build_indices, break_up_masters, \
    generate_combinations_for_index, remove_keys_from_indices, UUID, INDEX_REFS, count_entities, deduplicate, run, \
    SNAPSHOTS, convert_lists_to_sets, LEVENSHTEIN_TOLERANCE, RULE_TO_SET_DICT, generate_indices, \
    add_snapshot_or_merged_master_to_new_master, DFS_SNAPSHOTS, merge_or_unmerge_values_for_each_key, KEY_SUFFIX, \
    LIST
from generator import generate, generate_records
from unittest.mock import patch

deduplicator.MERGES_FILE = './merges.json'


def init(indices, strategy):
    if not indices:
        for rule in strategy.split(";"):
            RULE_TO_SET_DICT[rule] = SortedSet(rule.split(','))
            indices[rule] = SortedDict()


class DeduplicatorTest(unittest.TestCase):
    def test_dfs_snapshots_to_reprocess(self):
        with open('dfs_snapshots_to_reprocess.json') as json_file:

            # given
            json_str = json_file.read()
            json_data = json.loads(json_str)
            snapshots_to_reprocess = []

            # when
            dfs_snapshots_to_reprocess(json_data, snapshots_to_reprocess, json_data, set(),
                                       dict((s, {UUID: s}) for s in json_data[SNAPSHOTS]))

            # then
            self.assertTrue(len(snapshots_to_reprocess) == 4)
            self.assertTrue(len(set([s['uuid'] for s in snapshots_to_reprocess])) == 4)
            for snapshot in snapshots_to_reprocess:
                for param in AUX_PARAMS:
                    self.assertFalse(snapshot.get(param, None))

    def test_break_up_master(self):
        # before
        self.dfs_snapshots_to_reprocess = deduplicator.dfs_snapshots_to_reprocess

        # given
        indices = {}
        snapshots_dict = {}
        deduplicator.dfs_snapshots_to_reprocess = \
            Mock(side_effect=lambda a, b, c, d, e: (b.extend([{UUID: '1'}, {UUID: '2'}, {UUID: '3'}, {UUID: '4'}]), 0)[
                1])
        with open('snapshots.json') as json_file:
            json_str = json_file.read()
            json_data = json.loads(json_str)
            init(indices, '3,6,8;1;10;4,9')
            indices, _, _ = build_indices(json_data, indices, snapshots_dict)
        with open('changed_snapshot.json') as json_file:
            json_str = json_file.read()
            entity = json.loads(json_str)

            # when
            snapshots_to_reprocess = break_up_master(entity['1'], indices, snapshots_dict, {})

            # then
            self.assertTrue(len(snapshots_to_reprocess[0]) == 5)

        # after
        deduplicator.dfs_snapshots_to_reprocess = self.dfs_snapshots_to_reprocess

    def test_break_up_masters(self):
        # before
        self.break_up_master = deduplicator.break_up_master

        # given
        deduplicator.break_up_master = Mock(side_effect=[({1: '1', 2: '2', 3: '3'}, 0), ({4: '4', 5: '5', 6: '6'}, 1)])

        # when then
        self.assertTrue(len(break_up_masters({}, {'1': 1, '2': 2}, {}, {})[0]) == 6)

        # after
        deduplicator.break_up_master = self.break_up_master

    def test_build_indices(self):
        # given
        rounds = 10
        num_records = 10000
        file_name = 'test_data.json'
        for i in range(rounds):
            generate(num_records, 5, 15, 555, file_name, 'new')
            strategies = '300,600,800;100;1000;400,900'
            with open(file_name) as json_file:
                indices = {}
                snapshots_dict = {}
                json_str = json_file.read()
                json_data = json.loads(json_str)
                init(indices, strategies)

                # when
                indices, entities_in_indices, skipped_entities_num = build_indices(json_data, indices, snapshots_dict)
                all_entities_in_indices = set([e for _, index in indices.items() for _, e in index.items()])

                # then
                self.assertTrue(len(all_entities_in_indices) > 0)
                self.assertTrue(len(all_entities_in_indices | skipped_entities_num) == num_records)

    def test_generate_combinations_for_index(self):
        # given
        rounds = 100
        num_records = 100
        num_dimensions = 51
        num_min_dimensions = int(num_dimensions / 3)
        values_range_up_to = num_dimensions ** 2 * 2
        max_num_values = 5
        file_name = 'pseudo_master.json'
        for _ in range(rounds):
            rule_parameters_set = set([str(p) + '00' for p in random.sample(range(num_dimensions),
                                                                     random.randint(1, num_min_dimensions))])
            generate_records(num_records, num_min_dimensions, num_dimensions, values_range_up_to, file_name,
                             None, max_num_values)
            with open(file_name) as json_file:
                json_str = json_file.read()
                json_data = json.loads(json_str)
                for _, entity in json_data.items():
                    entity_to_merge_into_key_set = set(entity.keys())

                    if rule_parameters_set <= entity_to_merge_into_key_set:
                        # when
                        result = generate_combinations_for_index(entity, entity_to_merge_into_key_set,
                                                                 rule_parameters_set)

                        # then
                        actual = sum(len(set(a)) for a in result)
                        expected_list = [len(set(v)) for k, v in entity.items() if k in rule_parameters_set]
                        expected = len(expected_list)
                        self.assertEqual(expected if expected == 0 else reduce(lambda x, y: x * y, expected_list),
                                         actual)

    def test_generate_indices(self):
        # given
        rounds = 1000
        num_records = 100
        num_dimensions = 51
        num_min_dimensions = int(num_dimensions / 3)
        values_range_up_to = num_dimensions ** 2 * 2
        max_num_values = 5
        rules_number = 7
        file_name = 'pseudo_master.json'
        indices = {}
        for i in range(rules_number):
            rule = [str(p) + '00' for p in random.sample(
                    range(num_dimensions), random.randint(1, num_min_dimensions))]
            rule_str = ','.join(rule)
            RULE_TO_SET_DICT[rule_str] = set(rule)
            indices[rule_str] = SortedDict()

        index_counter = 0
        for _ in range(rounds):

            generate_records(num_records, num_min_dimensions, num_dimensions, values_range_up_to, file_name,
                             None, max_num_values)
            with open(file_name) as json_file:
                json_str = json_file.read()
                json_data = json.loads(json_str)
                for _, entity in json_data.items():
                    # when
                    key_set = entity.keys()
                    generate_indices(indices, entity)

                    # then
                    actual_index_count = len([self.assertEqual(indices[b[0]][c], entity['uuid'])
                                  for b in entity.get('index_refs', {}).items() for c in b[1]])
                    index_counter += actual_index_count
                    expected_rule_index_counter_list = \
                        [reduce(lambda b, c: b * c, [len(entity[d]) for d in a.split(',')], 1)
                                     if a.split(',') else 0 for a in indices.keys() if set(a.split(',')) <= key_set]
                    expected_index_count = reduce(lambda a, b: a + b, expected_rule_index_counter_list, 0)
                    self.assertEqual(expected_index_count, actual_index_count)
                    expected_index_counter = reduce(lambda a, b: a + b, [len(c) for c in indices.values()])
                    self.assertEqual(expected_index_counter, index_counter)

    def test_add_snapshot_or_merged_master_to_new_master(self):
        # given
        rounds = 100
        num_records = 100
        num_dimensions = 51
        num_min_dimensions = int(num_dimensions / 3)
        values_range_up_to = num_dimensions ** 2 * 2
        max_num_values = 5
        file_name = 'pseudo_master.json'
        prev_entity = {UUID: 'first'}

        snapshot_counter = 0
        for _ in range(rounds):
            generate_records(num_records, num_min_dimensions, num_dimensions, values_range_up_to, file_name,
                             None, max_num_values)
            with open(file_name) as json_file:
                json_str = json_file.read()
                json_data = json.loads(json_str)
                for _, entity in json_data.items():
                    # when
                    snapshot_counter += 1
                    add_snapshot_or_merged_master_to_new_master(entity, prev_entity, 'a', 'b')

                    # then
                    self.assertEqual(snapshot_counter, len(entity[DFS_SNAPSHOTS]))
                    prev_entity = entity

    def test_merge_or_unmerge_values_for_each_key(self):
        # given
        rounds = 30
        num_records = 100
        num_dimensions = 51
        num_min_dimensions = int(num_dimensions / 3)
        values_range_up_to = num_dimensions ** 2 * 2
        max_num_values = 5
        file_name = 'pseudo_master.json'
        prev_entity = {UUID: 'first', '100': {1}, 'l_100': [1]}
        value_dict = {'100': {1}}

        expected_value_count = 1
        for _ in range(rounds):
            generate_records(num_records, num_min_dimensions, num_dimensions, values_range_up_to, file_name,
                             None, max_num_values)
            with open(file_name) as json_file:
                json_str = json_file.read()
                json_data = json.loads(json_str)
                json_data = convert_lists_to_sets(json_data)
                for _, entity in json_data.items():
                    temp_dict = {}
                    for param in entity:
                        value = entity[param]
                        if param.endswith(KEY_SUFFIX) and not param.startswith(LIST):
                            temp_dict[LIST + param] = list(value)
                            value_dict[param] = value_dict.get(param, set()) | value
                    for param in temp_dict:
                        value = temp_dict[param]
                        if not entity.get(param, None):
                            entity[param] = value


                    # when
                    entity_values_count = reduce(lambda a, b: a + b, [len(c[1]) for c in entity.items()
                                                                      if KEY_SUFFIX in c[0] and LIST in c[0]], 0)
                    expected_value_count += entity_values_count
                    merge_or_unmerge_values_for_each_key(prev_entity, entity, True)

                    # then
                    actual_value_count = reduce(lambda a, b: a + b, [len(c[1]) for c in entity.items()
                                                                     if KEY_SUFFIX in c[0] and LIST in c[0]])
                    self.assertEqual(expected_value_count, actual_value_count)

                    prev_entity = entity

        for a in value_dict:
            self.assertEqual(value_dict[a], prev_entity[a], msg=a)


    def test_remove_keys_from_indices(self):
        # given
        rounds = 100
        num_records = 100
        num_dimensions = 51
        num_min_dimensions = int(num_dimensions / 3)
        values_range_up_to = num_dimensions ** 2 * 2
        max_num_values = 5
        strategies = '300,600,800;100;1000;400,900'
        file_name = 'pseudo_master.json'
        for _ in range(rounds):
            generate_records(num_records, num_min_dimensions, num_dimensions, values_range_up_to, file_name,
                             None, max_num_values)
            with open(file_name) as json_file:
                json_str = json_file.read()
                json_data = json.loads(json_str)
                indices = {}
                snapshots = {}
                init(indices, strategies)
                build_indices(json_data, indices, snapshots)
                for _, entity in json_data.items():

                    # when
                    remove_keys_from_indices(indices, entity)
                    for _, index in indices.items():
                        for _, entity_id in index.items():
                            # then
                            self.assertNotEqual(entity[UUID], entity_id)
                    self.assertFalse(entity.get(INDEX_REFS, None))

    def test_count_entities(self):
        # given
        rounds = 100
        num_records = 100
        num_dimensions = 51
        num_min_dimensions = int(num_dimensions / 3)
        values_range_up_to = num_dimensions ** 2 * 2
        max_num_values = 5
        file_name = 'count_entities.json'
        for _ in range(rounds):
            generate_records(num_records, num_min_dimensions, num_dimensions, values_range_up_to, file_name,
                             None, max_num_values)
            with open(file_name) as json_file:
                json_str = json_file.read()
                json_data = json.loads(json_str)
                strategies = '300,600,800;100;1000;400,900'
                entities_set = set()
                snapshots = {}
                indices = {}
                init(indices, strategies)
                indices, entities_in_indices, _ = build_indices(json_data, indices, snapshots)

                # when
                count_entities(indices, entities_set, dict((s[UUID], s) for _, s in json_data.items()))

                # then
                self.assertEqual(len(entities_in_indices), len(entities_set))

    def test_deduplicate(self):
        # given
        rounds = 100
        num_records = 100
        num_dimensions = 51
        num_min_dimensions = int(num_dimensions / 3)
        values_range_up_to = num_dimensions ** 2 * 2
        max_num_values = 5
        file_name = 'deduplicate.json'
        for _ in range(rounds):
            generate_records(num_records, num_min_dimensions, num_dimensions, values_range_up_to, file_name,
                             None, max_num_values)
            with open(file_name) as json_file:
                json_str = json_file.read()
                json_data = json.loads(json_str)
                json_data = convert_lists_to_sets(json_data)
                strategies = '300,600,800;100;1000;400,900'

                merges = {}
                for rule in strategies.split(';'):
                    merges[rule] = 0

                indices = {}
                init(indices, strategies)

                # when
                _, master_records_and_total_merge_counter, entities_in_indices_len, _, _ = \
                    deduplicate(json_data, indices, {}, set(), 0, merges)

                # then
                self.assertEqual(master_records_and_total_merge_counter, entities_in_indices_len)

    def test_run(self):
        # given
        rounds = 100
        num_records = 100
        num_dimensions = 51
        num_min_dimensions = int(num_dimensions / 3)
        values_range_up_to = num_dimensions ** 2 * 2
        max_num_values = 5
        file_name = 'run.json'

        for _ in range(rounds):
            with patch('deduplicator.deduplicate', CaptureValues(deduplicate)) as deduplicate_spy:
                # before
                _temp_get_input = deduplicator.get_input
                _temp_read = redisadapter.RedisDict.read
                _temp_ping = redis.client.StrictRedis.ping
                _temp_sync = redisadapter.RedisDict.sync
                deduplicator.get_input = Mock(side_effect=[file_name, 'exit'])
                redisadapter.RedisDict.read = Mock()
                redis.client.StrictRedis.ping = Mock()
                redisadapter.RedisDict.sync = Mock()
                deduplicator.get_dict = dict
                curse(dict, "sync", lambda x: None)
                generate_records(num_records, num_min_dimensions, num_dimensions, values_range_up_to, file_name,
                                 None, max_num_values)
                strategies = '300,600,800;100;1000;400,900'

                # when
                run(strategies)

                # then
                master_records_set_and_total_merge_counter = deduplicate_spy.return_values[0][1]
                entities_in_indices_len = deduplicate_spy.return_values[0][2]
                self.assertEqual(master_records_set_and_total_merge_counter, entities_in_indices_len)

                # after
                deduplicator.get_input = _temp_get_input
                redisadapter.RedisDict.read = _temp_read
                redis.client.StrictRedis.ping = _temp_ping
                redisadapter.RedisDict.sync = _temp_sync

    def test_run_multi(self):
        # given
        rounds = 100
        num_records = 100
        num_dimensions = 51
        num_min_dimensions = int(num_dimensions / 3)
        values_range_up_to = 10
        max_num_values = 5
        file_name = 'run_multi'
        num_files = 5

        for _ in range(rounds):
            with patch('deduplicator.deduplicate', CaptureValues(deduplicate)) as deduplicate_spy:

                # before
                _temp_get_input = deduplicator.get_input
                _temp_read = redisadapter.RedisDict.read
                _temp_ping = redis.client.StrictRedis.ping
                _temp_sync = redisadapter.RedisDict.sync
                file_names = [file_name + '_' + str(i) + '.json' for i in range(num_files)]
                deduplicator.get_input = Mock(side_effect=file_names + ['exit'])
                redisadapter.RedisDict.read = Mock()
                redis.client.StrictRedis.ping = Mock()
                redisadapter.RedisDict.sync = Mock()
                deduplicator.get_dict = dict
                curse(dict, "sync", lambda x: None)
                for f_n in file_names:
                    generate_records(num_records, num_min_dimensions, num_dimensions, values_range_up_to, f_n, None,
                                     max_num_values)
                strategies = '300,600;400'
                run(strategies)
                master_records_set_and_total_merge_counter = deduplicate_spy.return_values[0][1]

                # when
                entities_in_indices_len = deduplicate_spy.return_values[0][2]

                # then
                self.assertEqual(master_records_set_and_total_merge_counter, entities_in_indices_len)

                # after
                deduplicator.get_input = _temp_get_input
                redisadapter.RedisDict.read = _temp_read
                redis.client.StrictRedis.ping = _temp_ping
                redisadapter.RedisDict.sync = _temp_sync

    def test_run_change(self):
        # given
        rounds = 100
        num_records = 100
        num_dimensions = 51
        num_min_dimensions = int(num_dimensions / 3)
        values_range_up_to = 10
        max_num_values = 5
        file_name = 'run_multi'
        num_files = 5

        for _ in range(rounds):
            with patch('deduplicator.deduplicate', CaptureValues(deduplicate)) as deduplicate_spy:

                # before
                _temp_get_input = deduplicator.get_input
                _temp_read = redisadapter.RedisDict.read
                _temp_ping = redis.client.StrictRedis.ping
                _temp_sync = redisadapter.RedisDict.sync
                file_names = [file_name + '_' + str(i) + '.json' for i in range(num_files)]
                deduplicator.get_input = Mock(side_effect=file_names + ['change', 'changes.json', 'exit'])
                redisadapter.RedisDict.read = Mock()
                redis.client.StrictRedis.ping = Mock()
                redisadapter.RedisDict.sync = Mock()
                deduplicator.get_dict = dict
                curse(dict, "sync", lambda x: None)
                for f_n in file_names:
                    generate_records(num_records, num_min_dimensions, num_dimensions, values_range_up_to, f_n, None,
                                     max_num_values)
                generate(num_records, num_min_dimensions, num_dimensions, values_range_up_to, ','.join(file_names),
                         'change')
                strategies = '300,600;400'

                # when
                run(strategies)
                for result in deduplicate_spy.return_values:
                    master_records_set_and_total_merge_counter = result[1]
                    entities_in_indices_len = result[2]

                    # then
                    self.assertEqual(master_records_set_and_total_merge_counter, entities_in_indices_len)

                # after
                deduplicator.get_input = _temp_get_input
                redisadapter.RedisDict.read = _temp_read
                redis.client.StrictRedis.ping = _temp_ping
                redisadapter.RedisDict.sync = _temp_sync

    def test_levenshtein_tolerance_constant(self):
        self.assertFalse(fuzz.token_sort_ratio('Бунин Сергей', 'Сергей Бунин') <= LEVENSHTEIN_TOLERANCE)
        self.assertFalse(fuzz.token_sort_ratio('Бунин Сергей Сергеевич', 'Сергей Сергеевич Бунин') <= LEVENSHTEIN_TOLERANCE)
        self.assertFalse(fuzz.token_sort_ratio('Бунин Сергей Серкеевич', 'Сергей Сергеевич Бунин') <= LEVENSHTEIN_TOLERANCE)
        self.assertFalse(fuzz.token_sort_ratio('Сергей Бунин', 'Сергей Бунин') <= LEVENSHTEIN_TOLERANCE)
        self.assertTrue(fuzz.token_sort_ratio('Сергей Бунин', 'Сергей Сергеевич Бунин') <= LEVENSHTEIN_TOLERANCE)
        self.assertFalse(fuzz.token_sort_ratio('Сергей Серкеевич Бунин', 'Сергей Сергеевич Бунин') <= LEVENSHTEIN_TOLERANCE)
        self.assertFalse(fuzz.token_sort_ratio('Сергей Иванович Бунин', 'Сергей Сергеевич Бунин') <= LEVENSHTEIN_TOLERANCE)
        self.assertTrue(fuzz.token_sort_ratio('Сергей Иванович', 'Сергей Сергеевич') <= LEVENSHTEIN_TOLERANCE)
        self.assertTrue(fuzz.token_sort_ratio('Сергей Серкеевич Бунин', 'Сергей Бунин') <= LEVENSHTEIN_TOLERANCE)
        self.assertTrue(fuzz.token_sort_ratio('Сергей Серкеевич Бунин', 'Сергей Букин') <= LEVENSHTEIN_TOLERANCE)
        self.assertFalse(fuzz.token_sort_ratio('Сергей Серкеевич Бунин Ахры', 'Сергей Бунин Ахры') <= LEVENSHTEIN_TOLERANCE)



class CaptureValues(object):
    def __init__(self, func):
        self.func = func
        self.return_values = []

    def __call__(self, *args, **kwargs):
        answer = self.func(*args, **kwargs)
        self.return_values.append(answer)
        return answer


if __name__ == '__main__':
    unittest.main(buffer=True)
