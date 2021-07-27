from typing import Optional
import time

from minerva.commands.trend_store import materialize_all, process_modified_log


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'live-monitor',
        help='live monitoring for materializations after initialization'
    )

    cmd.add_argument(
        "--poll-timeout", default=2, type=float,
        help="Time to wait between poll cycles (in seconds)"
    )

    cmd.add_argument(
        "--batch-size", default=50, type=int,
        help="Number of materializations per batch (0=No limit)"
    )

    cmd.set_defaults(cmd=live_monitor_cmd)


def live_monitor_cmd(args):
    print('Live monitoring for materializations')

    if args.batch_size == 0:
        batch_size = None
    else:
        batch_size = args.batch_size

    try:
        live_monitor(batch_size, args.poll_timeout)
    except KeyboardInterrupt:
        print("Stopped")


def live_monitor(batch_size: Optional[int], poll_timeout: float):
    while True:
        process_modified_log(False)
        materialize_all(False, batch_size, False)

        time.sleep(poll_timeout)
