
import time

from minerva.commands.trend_store import materialize_all, process_modified_log


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'live-monitor',
        help='live monitoring for materializations after initialization'
    )

    cmd.set_defaults(cmd=live_monitor_cmd)


def live_monitor_cmd(args):
    print('Live monitoring for materializations')

    try:
        live_monitor()
    except KeyboardInterrupt:
        print("Stopped")


def live_monitor():
    max_num_materializations = 50

    while True:
        process_modified_log(False)
        materialize_all(False, max_num_materializations, False)

        time.sleep(2)
