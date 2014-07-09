postgresql:
  pkg:
    - installed
  service:
    - running

postgis:
  pkg:
    - installed

postgresql-9.3-postgis-scripts:
  pkg:
    - installed

vim:
  pkg.installed

zsh:
  pkg.installed

python-virtualenv:
  pkg.installed

language-pack-nl:
  pkg.installed

vagrant:
  user.present:
    - shell: /bin/zsh

  postgres_user.present:
    - login: true
    - superuser: true

minerva:
  postgres_database.present

/home/vagrant/.zshrc:
  file.managed:
    - source: ~
    - user: vagrant
    - group: vagrant
    - mode: 644

init-minerva-db:
  cmd.wait:
    - name: '/shared/schema/run-scripts /shared/schema/scripts'
    - user: vagrant
    - env:
      - PGDATABASE: minerva
    - watch:
      - postgres_database: minerva
