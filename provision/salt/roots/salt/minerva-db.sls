postgresql-repo:
  pkgrepo:
    - managed
    - name: deb http://apt.postgresql.org/pub/repos/apt/ trusty-pgdg main
    - key_url: https://www.postgresql.org/media/keys/ACCC4CF8.asc
    - file: /etc/apt/sources.list.d/pgdg.list

postgresql-9.4:
  pkg:
    - installed

postgresql:
  service:
    - running

postgresql-server-dev-9.4:
  pkg:
    - installed

libpq-dev:
  pkg:
    - installed

language-pack-nl:
  pkg:
    - installed

python3-pip:
  pkg:
    - installed

python-pip:
  pkg:
    - installed

# Psycopg2 requires compilation, so it is easier to use the standard Ubuntu
# package
python3-psycopg2:
  pkg:
    - installed

python-package:
  pip.installed:
    - editable: /vagrant/
    - bin_env: /usr/bin/pip3
    - require:
      - pkg: python3-psycopg2
      - pkg: python3-pip

# python_dateutil from pypi currently has permission issues with some files
# after installation, so use the standard Ubuntu package
python3-dateutil:
  pkg:
    - installed

vagrant:
  user.present:
    - shell: /bin/zsh

  postgres_user.present:
    - login: True
    - superuser: True
    - require:
      - service: postgresql

create-database:
  cmd.wait:
    - name: '/vagrant/provision/salt/roots/salt/resources/commands/create-database'
    - user: vagrant
    - env:
      - MINERVA_DB_NAME: minerva
      - MINERVA_DB_SCRIPT_ROOT: '/vagrant/schema/scripts'
    - watch:
      - postgres_user: vagrant

install-pgtap:
  cmd.wait:
    - name: '/vagrant/provision/salt/roots/salt/resources/commands/install-pgtap'
    - watch:
      - postgres_user: vagrant
    - env:
      - PGDATABASE: minerva
    - user: vagrant
    - require:
      - pkg: git

git:
  pkg:
    - installed

/etc/minerva/instances/default.conf:
  file.managed:
    - source: salt://resources/minerva_instance.conf
    - makedirs: True

/etc/postgresql/9.4/main/postgresql.conf:
  file.append:
    - text: 'minerva.trigger_mark_modified = on'
    - require:
      - pkg: postgresql-9.4
