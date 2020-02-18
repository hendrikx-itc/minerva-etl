import os
import errno
import sys
import io

from pkg_resources import resource_listdir, resource_isdir, resource_stream

from minerva.instance import INSTANCE_ROOT_VARIABLE

from jinja2 import Template


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'quick-start',
        help='command for setting up a Minerva instance skeleton'
    )

    cmd.add_argument(
        '--instance-name', default='default', help='Set the instance name'
    )

    cmd.add_argument(
        'instance_root', nargs='?', help='Root directory for instance skeleton'
    )

    cmd.set_defaults(cmd=quick_start_cmd)


def quick_start_cmd(args):
    instance_root = os.path.abspath(
        args.instance_root or os.environ.get(INSTANCE_ROOT_VARIABLE) or os.getcwd()
    )

    sys.stdout.write(
        f"Creating Minerva instance skeleton in '{instance_root}'\n"
    )

    try:
        create_skeleton(instance_root, args.instance_name)
    except Exception as exc:
        sys.stdout.write("Error:\n{}".format(str(exc)))
        raise exc


def create_skeleton(instance_root, instance_name):
    package_name = 'minerva'
    path = 'instance/resources'

    data = {
        'instance_name': instance_name,
        'author': 'The Author',
        'version': '1.0',
        'release': '1.0.0'
    }

    copy_resource_tree(data, instance_root, package_name, path)


def copy_resource_tree(data, target_dir, package_name, resource_root,
                       resource_dir=''):
    try:
        os.makedirs(os.path.join(target_dir, resource_dir))
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    for resource in resource_listdir(package_name,
                                     os.path.join(resource_root,
                                                  resource_dir)):
        resource_path = os.path.join(resource_root, resource_dir, resource)

        if resource_isdir(package_name, resource_path):
            copy_resource_tree(data, target_dir, package_name, resource_root,
                               os.path.join(resource_dir, resource))
        else:
            stream = resource_stream(package_name, resource_path)

            template = Template(io.TextIOWrapper(stream, 'utf-8').read())

            target_path = os.path.join(target_dir, resource_dir, resource)

            if os.path.isfile(target_path):
                print(f"Skipping '{target_path}' because it already exists")
            else:
                with open(target_path, 'wb') as out_file:
                    print(f"Creating '{target_path}'")
                    out_file.write(template.render(**data).encode('utf-8'))
