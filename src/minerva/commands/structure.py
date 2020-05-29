def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'structure', help='command for dumping or loading Minerva structure'
    )

    cmd_subparsers = cmd.add_subparsers()

    setup_dump_parser(cmd_subparsers)
    setup_load_parser(cmd_subparsers)


def setup_dump_parser(subparsers):
    cmd = subparsers.add_parser(
        'dump', help='command for dumping Minerva structure'
    )

    cmd.set_defaults(cmd=dump_structure_cmd)


def dump_structure_cmd(args):
    print("dump")


def setup_load_parser(subparsers):
    cmd = subparsers.add_parser(
        'load', help='command for loading Minerva structure'
    )

    cmd.set_defaults(cmd=load_structure_cmd)


def load_structure_cmd(args):
    print("load")
