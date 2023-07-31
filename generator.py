import json
import random
import sys
import uuid

UUID = 'uuid'
CHANGES_FILE = 'changes.json'


def generate(num_records, num_min_dimensions, num_dimensions, values_range_up_to, paths, what):
    if what == 'new':
        path = paths
        generate_records(num_records, num_min_dimensions, num_dimensions, values_range_up_to, path, None, 1)
    else:
        all_data = {}
        for path in paths.split(','):
            with open(path) as json_file:
                json_str = json_file.read()
                json_data = json.loads(json_str)
                all_data |= json_data
        ids = [entity[UUID] for entity in all_data.values()]
        ids = random.sample(ids, int(num_records))
        generate_records(num_records, num_min_dimensions, num_dimensions, values_range_up_to, CHANGES_FILE, ids, 1)


def generate_records(num_records, num_min_dimensions, num_dimensions, values_range_up_to, path_where_to_write, ids,
                     max_num_values):
    dictionary = {}
    values = range(int(values_range_up_to))
    for i in range(int(num_records)):
        inner = {}
        rand_subset = random.sample(range(1, int(num_dimensions) + 1), int(num_min_dimensions))
        rand_subset.sort()
        for r in rand_subset:
            inner[str(r) + '00'] = tuple([random.sample(values, 1)[0] for _ in range(random.randint(1, max_num_values))])
        inner[UUID] = str(uuid.uuid4()) if ids is None else ids[i]
        dictionary[i] = inner
    with open(path_where_to_write, 'w', encoding='utf-8') as f:
        json.dump(dictionary, f, ensure_ascii=False, indent=4)


if __name__ == '__main__':
    generate(*sys.argv[1:])
