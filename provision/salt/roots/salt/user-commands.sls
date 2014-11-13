/home/vagrant/bin/recreate-database:
  file:
    - managed
    - source: salt://resources/recreate-database
    - user: vagrant
    - group: vagrant
    - mode: 755
    - makedirs: True

/home/vagrant/bin/create-database:
  file:
    - managed
    - source: salt://resources/create-database
    - user: vagrant
    - group: vagrant
    - mode: 755
    - makedirs: True
