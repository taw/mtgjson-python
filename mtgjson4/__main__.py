import argparse
import asyncio
import itertools
import sys
import time
from typing import Iterator, List

import aiohttp
import aioprocessing
import hanging_threads

from mtgjson4 import mtg_builder, mtg_global, mtg_storage

THREAD_MONITOR = hanging_threads.start_monitoring()


def main() -> None:
    """
    Main method that starts the entire build process
    :param args:
    :param loop:
    :param session:
    :param language_to_build:
    :return:
    """

    # Main Applied
    mtg_storage.ensure_set_dir_exists()

    async def example(queue, event, lock, set_name, all_sets_to_build):
        await event.coro_wait()
        async with lock:
            json_builder = mtg_builder.MTGJSON(all_sets_to_build)
            json_builder.build_set(set_name, 'en')
            await queue.coro_put(None)

    loop = asyncio.get_event_loop()
    queue = aioprocessing.AioQueue()
    lock = aioprocessing.AioLock()
    event = aioprocessing.AioEvent()
    tasks = [asyncio.ensure_future(example(queue, event, lock, set_name, SETS_TO_BUILD)) for set_name in SETS_TO_BUILD]
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()
    return

if __name__ == '__main__':
    # Start by processing all arguments to the program
    arg_parser = argparse.ArgumentParser(description=mtg_global.DESCRIPTION)

    arg_parser.add_argument('-v', '--version', action='store_true', help='MTGJSON version information')

    arg_parser.add_argument(
        '-s',
        '--sets',
        metavar='SET',
        nargs='+',
        type=str,
        help='A list of sets to build. Will be ignored if used with --all-sets.')

    arg_parser.add_argument(
        '-a',
        '--all-sets',
        action='store_true',
        help='Build all sets found in the set_configs directory, including sub-directories.')

    arg_parser.add_argument(
        '-f',
        '--full-out',
        action='store_true',
        help='Create the AllCards, AllSets, and AllSetsArray files based on the sets found in the set_outputs '
        'directory. ')

    arg_parser.add_argument(
        '-l',
        '--language',
        default=['en'],
        metavar='LANG',
        type=str,
        nargs=1,
        help=
        'Build foreign language version of a specific set. The english version must have been built already for this '
        'flag to be used. ')

    arg_parser.add_argument(
        '--max-sets-build',
        default=[5],
        metavar='#',
        type=int,
        nargs=1,
        help='You can limit how many sets will be built at one time. The higher the number, the more memory '
        'consumption. If not enough memory, swap space will be used, which slows down the program tremendiously. '
        'Defaults to 5. 0 to Disable. ')

    # If user supplies no arguments, show help screen and exit
    if len(sys.argv) == 1:
        arg_parser.print_help(sys.stderr)
        exit(1)

    cl_args = vars(arg_parser.parse_args())
    lang_to_process = cl_args['language'][0]

    # Get version info and exit
    if cl_args['version']:
        print(mtg_global.VERSION_INFO)
        exit(0)

    # Ensure the language is a valid language, otherwise exit
    if mtg_global.get_language_long_name(lang_to_process) is None:
        print('MTGJSON: Language \'{}\' not supported yet'.format(lang_to_process))
        exit(1)

    # If only full out, just build from what's there and exit
    if (cl_args['sets'] is None) and (not cl_args['all_sets']) and cl_args['full_out']:
        mtg_builder.create_combined_outputs()
        exit(0)

    # Global of all sets to build
    SETS_TO_BUILD = mtg_builder.determine_gatherer_sets(cl_args)

    # Start the build process
    start_time = time.time()

    main()

    if cl_args['full_out']:
        mtg_builder.create_combined_outputs()

    end_time = time.time()
    print('Time: {}'.format(end_time - start_time))
