from contextlib import closing

from psycopg2 import connect


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'entitytype', help='command for administering entity types'
    )

    group = cmd.add_mutually_exclusive_group(required=False)

    group.add_argument(
        '--create', '-c', action='store_true', help='create new entity type'
    )

    group.add_argument(
        '--delete', '-d', action='store_true', help='delete entity type'
    )

    cmd.add_argument('name', help='name of the entity type')

    cmd.set_defaults(cmd=entitytype_cmd)


def entitytype_cmd(args):
    if args.create:
        create_entity_type(args.name)
    elif args.delete:
        delete_entity_type(args.name)


def create_entity_type(name):
    query_args = (name,)

    with closing(connect('')) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute('SELECT directory.create_entity_type(%s)', query_args)

        conn.commit()


def delete_entity_type(name):
    query_args = (name,)

    with closing(connect('')) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute('SELECT directory.delete_entity_type(%s)', query_args)

            rowcount = cursor.rowcount

        conn.commit()

    if rowcount == 1:
        print('successfully deleted entity type {}'.format(name))
