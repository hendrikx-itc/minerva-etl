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

/home/vagrant/bin/run-db-tests:
  file:
    - managed
    - source: salt://resources/run-db-tests
    - user: vagrant
    - group: vagrant
    - mode: 755
    - makedirs: True

/home/vagrant/bin/build-db-docs:
  file:
    - managed
    - source: salt://resources/build-db-docs
    - user: vagrant
    - group: vagrant
    - mode: 755
    - makedirs: True
