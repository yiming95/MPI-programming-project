import argparse
import json
import traceback
from collections import Counter
from mpi4py import MPI

parser = argparse.ArgumentParser(description='HPC')

parser.add_argument('grid', help="Grid file location")
parser.add_argument('data', help="Data file location")

"""
Data structure

instances sent to worker [[tag1, tag2, ...], [x, y], 1]
returned from worker: dict <key: grid_id, val: [num_posts, Counter([])]>

"""

args = parser.parse_args()

GRID_PATH = args.grid
DATA_PATH = args.data

GRID_FP = open(GRID_PATH)
GRID = json.load(GRID_FP)

BUFFER_SIZE = 5000  # Size of datablock sent to worker.


class FLAGS:
    END_OF_ITERATION = "400"  # Send END_OF_ITERATION to worker to stop


def point_in_grid(coord, feature):
    xmin = feature['properties']['xmin']
    ymin = feature['properties']['ymin']
    xmax = feature['properties']['xmax']
    ymax = feature['properties']['ymax']
    return (xmin <= coord[0] <= xmax) and (ymin <= coord[1] <= ymax)


def get_grid_id(coord):
    features = GRID['features']
    feature = list(filter(lambda x: point_in_grid(coord, x), features))
    if len(feature) == 0:
        return None
    else:
        return feature[0].get('properties').get('id')


def hash_proc(h):
    return '#' + h['text'].lower()


def verify_entry(item):
    return item.get('doc') is not None and \
           item.get('doc').get('coordinates') is not None and \
           item.get('doc').get('entities') is not None and \
           item.get('doc').get('entities').get('hashtags') is not None and \
           item.get('doc').get('coordinates').get('coordinates') is not None


def data_feeder(file_path):
    """
    Generator which yields a chunk data of size BUFFER_SIZE at one time

    :param file_path: PATH of json file
    :yield: a data chunk
    """
    LINE_COUNT = 0
    fp = open(file_path, mode='r')
    # chunk = [None] * BUFFER_SIZE
    fp.readline()
    while True:
        try:
            chunk = [None] * BUFFER_SIZE
            for i in range(BUFFER_SIZE):
                next_item = json.loads(fp.readline().replace(',\n', '\n'))
                LINE_COUNT += 1

                if not next_item['doc']['retweeted']:
                    if verify_entry(next_item):
                        chunk[i] = (list(map(hash_proc, next_item['doc']['entities']['hashtags'])),
                                    next_item['doc']['coordinates']['coordinates'],
                                    1)
                else:
                    chunk[i] = None
                # ([tag1, tag2, tag3...], [x, y], count)

            yield chunk

        except StopIteration:
            yield chunk
            break
        except json.decoder.JSONDecodeError:
            # Last row of file raises this Error
            yield chunk
            break
        except Exception as e:
            break


def get_scatter_batch(g, num_processes):
    """
    Generate a batch of data for scattering
    :param num_processes: Number of processes including master
    :param g: Data feeding generator
    :return: a list of chunks of size num_processes
    """
    res = [None] * num_processes
    for i in range(num_processes):
        try:
            res[i] = next(g)
        # print(res[i])
        except StopIteration:
            pass
    return res


def test_feed_find():
    """
    For testing purpose
    :return:
    """
    g = data_feeder('../data/tinyTwitter.json')
    next_chunk = next(g)
    for d in next_chunk:
        print(get_grid_id(d[1]))


def reduce(chunk):
    """
    Executed on workers. Reduce the single instances by doing countings
    on posts and tags
    :param chunk: Data chunk sent from master to worker
    :return: A dictionary with size specified at the beginning of this file
    """
    reduced_dict = {}
    if chunk is None:
        return reduced_dict
    for e in chunk:
        if e is not None:
            grid_id = get_grid_id(e[1])
            if grid_id is not None:
                if reduced_dict.get(grid_id) is None:
                    reduced_dict[grid_id] = [0, Counter()]

                reduced_dict[grid_id][0] += e[2]
                reduced_dict[grid_id][1] += Counter(set(e[0]))

    return reduced_dict


def order(reduced_dictionary):
    """
    Sort the reduced dictionary
    :param reduced_dictionary:
    :return:
    """
    order_list = sorted(reduced_dictionary.items(), key=lambda x: x[1][0])
    order_list.reverse()

    return order_list


def combine(final_res: dict, reduced_list: list):
    """
    Combine results received from workers
    :param final_res:
    :param reduced_list: list of reduced_result, which len of num_workers
    :return: Return nothing. Side effect applied to update result
    """
    for entry in reduced_list:
        for k, v in entry.items():
            if final_res.get(k) is None:
                final_res[k] = [0, Counter()]

            final_res[k][0] += v[0]
            final_res[k][1] += v[1]


def verify(final_res: dict):
    """
    Verify the total number of posts
    :param final_res:
    :return:
    """
    post_sum = 0
    for k, v in final_res.items():
        post_sum += v[0]
        print(k, v[0])

    print(post_sum)


if __name__ == "__main__":
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    count = comm.Get_size()

    batch_num = 0

    g = data_feeder(DATA_PATH)
    final_res = {}
    if rank == 0:

        try:
            while True:
                batch = get_scatter_batch(g, count)
                data = comm.scatter(batch, root=0)
                reduced_dict = reduce(data)
                reduced_list = comm.gather(reduced_dict, root=0)
                combine(final_res, reduced_list)
                batch_num += 1

                if batch[0] is None:
                    # This is where nothing read from file
                    traceback.print_exc()
                    data = FLAGS.END_OF_ITERATION
                    batch = [data] * count
                    data = comm.scatter(batch)
                    break
        except Exception as e:
            traceback.print_exc()


    else:
        batch = None
        while True:
            data = comm.scatter(batch, root=0)

            if data == FLAGS.END_OF_ITERATION:
                break
            reduced_dict = reduce(data)
            comm.gather(reduced_dict, root=0)

    if rank == 0:
        final_res = order(final_res)
        for i in final_res:
            print(i[0], ': ', i[1][0], 'posts')

        for i in final_res:
            print(i[0], ': ', i[1][1].most_common(5))
